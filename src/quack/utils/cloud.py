#!/usr/bin/env python3

import fnmatch
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError, NoCredentialsError

from quack.exceptions import CloudStorageError


@dataclass
class CloudFileMetadata:
    path: str
    modified_time: datetime
    size: int


class CloudClient:
    """统一的云存储客户端（支持 OSS 和 S3，使用 boto3）"""

    def __init__(
        self,
        prefix: str,
        region: str,
        access_key_id: str,
        access_key_secret: str,
        endpoint: str,
    ):
        self._prefix = prefix

        # 解析协议和 bucket
        if prefix.startswith("oss://"):
            self._protocol = "oss"
            parts = prefix[6:].split("/", 1)
        elif prefix.startswith("s3://"):
            self._protocol = "s3"
            parts = prefix[5:].split("/", 1)
        else:
            raise ValueError(f"prefix 必须以 oss:// 或 s3:// 开头：{prefix}")

        self._bucket_name = parts[0]
        self._base_path = parts[1] if len(parts) > 1 else ""

        # 初始化 boto3 客户端
        try:
            if self._protocol == "oss":
                # 阿里云 OSS S3 兼容 API
                # 转换规则：在域名前加 s3. 前缀（如果还没有的话）
                # 例如：https://oss-cn-beijing-internal.aliyuncs.com
                #   -> https://s3.oss-cn-beijing-internal.aliyuncs.com
                if not endpoint:
                    raise ValueError("OSS 必须提供 endpoint")

                # 转换为 S3 兼容 endpoint（在域名前加 s3. 前缀，避免重复）
                s3_endpoint = endpoint.replace("://", "://s3.") if "://s3." not in endpoint else endpoint

                # 使用阿里云官方推荐的配置
                self._client = boto3.client(
                    "s3",
                    endpoint_url=s3_endpoint,
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=access_key_secret,
                    config=BotocoreConfig(
                        signature_version="s3",
                        s3={"addressing_style": "virtual"},
                    ),
                )
            elif endpoint:
                # 其他云存储（MinIO 等）使用自定义端点
                self._client = boto3.client(
                    "s3",
                    endpoint_url=endpoint,
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=access_key_secret,
                )
            else:
                # 使用标准 AWS S3
                self._client = boto3.client(
                    "s3",
                    region_name=region or "us-east-1",
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=access_key_secret,
                )
        except NoCredentialsError as e:
            raise CloudStorageError("云存储认证失败", str(e)) from e

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
            raise CloudStorageError(f"检查文件是否存在失败：{path}", str(e)) from e

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
        except ClientError as e:
            raise CloudStorageError(f"上传文件失败：{path}", str(e)) from e

    def download(self, path: str, dest: str) -> None:
        """下载文件或目录"""
        try:
            key = self._get_object_key(path)

            # 检查是否是目录（通过列出对象判断）
            objects = list(self._list_objects(key, max_keys=2))

            if not objects:
                raise CloudStorageError(f"云存储中不存在对象：{path}")

            # 如果 key 以 / 结尾或有多个对象，视为目录
            is_directory = key.endswith("/") or len(objects) > 1

            if is_directory:
                # 下载目录
                os.makedirs(dest, exist_ok=True)
                for obj in self._list_objects(key):
                    obj_key = obj["Key"]
                    # 计算本地路径
                    rel_path = obj_key[len(key) :].lstrip("/")
                    if not rel_path:
                        # 跳过目录对象本身
                        continue
                    local_file = os.path.join(dest, rel_path)
                    dir_path = os.path.dirname(local_file)
                    if dir_path:
                        os.makedirs(dir_path, exist_ok=True)
                    self._client.download_file(self._bucket_name, obj_key, local_file)
            else:
                # 下载单个文件
                dir_path = os.path.dirname(dest)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                self._client.download_file(self._bucket_name, key, dest)
        except ClientError as e:
            raise CloudStorageError(f"下载文件失败：{path}", str(e)) from e

    def read(self, path: str) -> str | None:
        """读取文件内容"""
        try:
            key = self._get_object_key(path)
            response = self._client.get_object(Bucket=self._bucket_name, Key=key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise CloudStorageError(f"读取文件失败：{path}", str(e)) from e

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
