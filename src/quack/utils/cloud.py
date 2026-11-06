#!/usr/bin/env python3

import fnmatch
import os
import re
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import NoReturn

import boto3
from boto3.exceptions import Boto3Error
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    NoCredentialsError,
    ProxyConnectionError,
    ReadTimeoutError,
)

from quack.config import CacheLimits, CloudConfig
from quack.exceptions import CacheSecurityError, CloudStorageError, CloudStorageTransientError

TRANSIENT_ERROR_CODES = {
    "500",
    "503",
    "IncompleteBody",
    "InternalError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
}

TRANSIENT_NETWORK_ERRORS = (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ProxyConnectionError,
    ReadTimeoutError,
)

CLIENT_ERROR_MESSAGE_RE = re.compile(r"An error occurred \(([^)]+)\) when calling the \w+ operation")


@dataclass
class CloudFileMetadata:
    path: str
    modified_time: datetime
    size: int


def _get_client_error_code(error: ClientError) -> str:
    code = error.response.get("Error", {}).get("Code", "")
    return str(code)


def _iter_error_chain(error: Exception) -> Iterator[Exception]:
    seen: set[int] = set()
    pending = [error]
    while pending:
        current_error = pending.pop()
        if id(current_error) in seen:
            continue

        seen.add(id(current_error))
        yield current_error

        pending.extend(
            wrapped_error
            for wrapped_error in [
                current_error.__context__,
                current_error.__cause__,
                getattr(current_error, "last_exception", None),
            ]
            if isinstance(wrapped_error, Exception)
        )


def _extract_error_code(error: Exception) -> str:
    for current_error in _iter_error_chain(error):
        if isinstance(current_error, ClientError):
            return _get_client_error_code(current_error)

        match = CLIENT_ERROR_MESSAGE_RE.search(str(current_error))
        if match:
            return match.group(1)

    return ""


def _is_transient_error(error: Exception) -> bool:
    if any(isinstance(current_error, TRANSIENT_NETWORK_ERRORS) for current_error in _iter_error_chain(error)):
        return True
    return _extract_error_code(error) in TRANSIENT_ERROR_CODES


def _raise_cloud_storage_error(message: str, path: str, error: Exception) -> NoReturn:
    code = _extract_error_code(error)
    exception_class = CloudStorageTransientError if _is_transient_error(error) else CloudStorageError
    raise exception_class(f"{message}：{path}", str(error), code=code or None) from error


class CloudClient:
    """统一的云存储客户端（支持 OSS 和 S3，使用 boto3）"""

    def __init__(self, config: CloudConfig, limits: CacheLimits):
        self._prefix = config.prefix
        self._limits = limits
        transport_config = BotocoreConfig(
            ignore_configured_endpoint_urls=True,
            connect_timeout=10,
            read_timeout=30,
            retries={"mode": "standard", "total_max_attempts": 2},
        )
        if config.prefix.startswith("oss://"):
            self._protocol = "oss"
            parts = config.prefix[6:].split("/", 1)
        elif config.prefix.startswith("s3://"):
            self._protocol = "s3"
            parts = config.prefix[5:].split("/", 1)
        else:
            raise ValueError("Cache prefix must begin with oss:// or s3://")
        if not parts[0]:
            raise ValueError("Cache bucket is required")
        self._bucket_name = parts[0]
        self._base_path = parts[1].rstrip("/") if len(parts) > 1 else ""
        credentials = {}
        if config.access_key_id:
            credentials = {
                "aws_access_key_id": config.access_key_id,
                "aws_secret_access_key": config.access_key_secret,
            }
            if config.session_token:
                credentials["aws_session_token"] = config.session_token
        endpoint = config.endpoint
        if self._protocol == "oss":
            if not endpoint:
                raise ValueError("OSS cache requires an endpoint")
            endpoint = endpoint.replace("://", "://s3.") if "://s3." not in endpoint else endpoint
            transport_config = transport_config.merge(
                BotocoreConfig(signature_version="s3", s3={"addressing_style": "virtual"})
            )
        region = config.region or ("auto" if ".r2.cloudflarestorage.com" in endpoint else "us-east-1")
        try:
            self._client = boto3.client(
                "s3",
                endpoint_url=endpoint or None,
                region_name=region,
                config=transport_config,
                **credentials,
            )
        except NoCredentialsError as error:
            raise CloudStorageError("Cache authentication failed") from error

    def _get_object_key(self, path: str) -> str:
        """将相对路径转换为对象键"""
        if self._base_path:
            return f"{self._base_path}/{path}"
        return path

    def _list_objects(self, prefix: str, max_keys: int | None = None) -> Iterator[dict]:
        """列出指定前缀下的对象"""
        paginator = self._client.get_paginator("list_objects_v2")
        count = 0
        pagination_config = {"PageSize": min(max_keys or 1000, 1000)} if max_keys else {}

        for page in paginator.paginate(Bucket=self._bucket_name, Prefix=prefix, PaginationConfig=pagination_config):
            for obj in page.get("Contents", []):
                yield obj
                count += 1
                if max_keys is not None and count >= max_keys:
                    return

    def exists(self, path: str) -> bool:
        """检查对象是否存在"""
        try:
            key = self._get_object_key(path)
            self._client.head_object(Bucket=self._bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            _raise_cloud_storage_error("检查文件是否存在失败", path, e)
        except (Boto3Error, BotoCoreError) as e:
            _raise_cloud_storage_error("检查文件是否存在失败", path, e)

    def upload(self, path: str, dest: str) -> None:
        """上传文件或目录"""
        try:
            key = self._get_object_key(dest)

            if os.path.isfile(path):
                # 上传单个文件
                self._client.upload_file(path, self._bucket_name, key)
            elif os.path.isdir(path):
                # 上传目录中的所有文件
                for root, _, files in os.walk(path):
                    for file in files:
                        local_file = os.path.join(root, file)
                        rel_path = os.path.relpath(local_file, path)
                        object_key = f"{key}/{rel_path}" if key else rel_path
                        self._client.upload_file(local_file, self._bucket_name, object_key)
            else:
                raise CloudStorageError(f"路径不存在或不是文件/目录：{path}")
        except (ClientError, Boto3Error, BotoCoreError) as e:
            _raise_cloud_storage_error("上传文件失败", path, e)

    def download(self, path: str, dest: str, *, max_bytes: int | None = None) -> None:
        """Download one exact object; neighboring keys never select directory mode."""
        temporary_path = None
        limit = max_bytes if max_bytes is not None else self._limits.max_archive_bytes
        try:
            response = self._client.get_object(Bucket=self._bucket_name, Key=self._get_object_key(path))
            body = response["Body"]
            try:
                if response["ContentLength"] > limit:
                    raise CacheSecurityError("Cache object exceeds its compressed size limit")
                os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
                with tempfile.NamedTemporaryFile(dir=os.path.dirname(os.path.abspath(dest)), delete=False) as output:
                    temporary_path = output.name
                    total = 0
                    while chunk := body.read(1024**2):
                        total += len(chunk)
                        if total > limit:
                            raise CacheSecurityError("Cache object exceeds its compressed size limit")
                        _ = output.write(chunk)
                os.replace(temporary_path, dest)
                temporary_path = None
            finally:
                body.close()
        except (ClientError, Boto3Error, BotoCoreError) as error:
            _raise_cloud_storage_error("Cache download failed", path, error)
        finally:
            if temporary_path is not None:
                os.unlink(temporary_path)

    def read(self, path: str) -> str | None:
        try:
            response = self._client.get_object(Bucket=self._bucket_name, Key=self._get_object_key(path))
            body = response["Body"]
            try:
                data = body.read(1024**2 + 1)
                if len(data) > 1024**2:
                    raise CacheSecurityError("Cache metadata exceeds its size limit")
                return data.decode("utf-8")
            finally:
                body.close()
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return None
            _raise_cloud_storage_error("Cache metadata read failed", path, error)
        except (Boto3Error, BotoCoreError) as error:
            _raise_cloud_storage_error("Cache metadata read failed", path, error)

    def remove(self, path: str, recursive=False) -> None:
        """删除对象"""
        try:
            key = self._get_object_key(path)

            if recursive:
                # 递归删除所有匹配的对象
                objects_to_delete = [{"Key": obj["Key"]} for obj in self._list_objects(key)]

                # 批量删除（每次最多 1000 个）
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    self._client.delete_objects(Bucket=self._bucket_name, Delete={"Objects": batch})
            else:
                # 删除单个对象
                self._client.delete_object(Bucket=self._bucket_name, Key=key)
        except ClientError as e:
            raise CloudStorageError(f"删除文件失败：{path}", str(e)) from e

    def filter_files(self, path: str, include: list[str], exclude: list[str]) -> list[CloudFileMetadata]:
        """列出并过滤文件"""
        try:
            key = self._get_object_key(path)
            result = []

            for obj in self._list_objects(key):
                obj_key = obj["Key"]
                # 获取相对路径
                rel_path = obj_key[len(self._base_path) :].lstrip("/") if self._base_path else obj_key

                # 简单的模式匹配（支持通配符）
                if include:
                    match_include = any(fnmatch.fnmatch(rel_path, pattern) for pattern in include)
                    if not match_include:
                        continue

                if exclude:
                    match_exclude = any(fnmatch.fnmatch(rel_path, pattern) for pattern in exclude)
                    if match_exclude:
                        continue

                result.append(
                    CloudFileMetadata(
                        path=rel_path,
                        modified_time=obj["LastModified"],
                        size=obj["Size"],
                    )
                )

            return result
        except ClientError as e:
            raise CloudStorageError(f"列出文件失败：{path}", str(e)) from e
