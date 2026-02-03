#!/usr/bin/env python3

import fnmatch
import os
import re
from dataclasses import dataclass
from datetime import datetime

import alibabacloud_oss_v2 as oss
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from quack.exceptions import CloudStorageError, ConfigError


@dataclass
class CloudFileMetadata:
    path: str
    modified_time: datetime
    size: int


class OSSClient:
    def __init__(
        self,
        prefix: str,
        _region: str,
        access_key_id: str,
        access_key_secret: str,
        endpoint: str,
    ):
        self._prefix = prefix

        if not prefix.startswith("oss://"):
            raise ValueError(f"OSS prefix 必须以 oss:// 开头：{prefix}")

        parts = prefix[6:].split("/", 1)
        self._bucket_name = parts[0]
        self._base_path = parts[1] if len(parts) > 1 else ""

        # 初始化 OSS 客户端
        if not all([access_key_id, access_key_secret, endpoint]):
            raise ValueError("使用 OSS SDK 需要提供 access_key_id、access_key_secret 和 endpoint 参数")

        # 如果没有明确指定 region，尝试从 endpoint 中提取
        region = _region
        if not region and endpoint:
            # 从 endpoint 中提取 region
            # 支持格式：oss-cn-beijing.aliyuncs.com 或 oss-cn-beijing-internal.aliyuncs.com
            match = re.search(r"oss-([a-z0-9-]+?)(?:-internal)?\.aliyuncs\.com", endpoint)
            if match:
                region = match.group(1)

        # 配置凭证提供者
        credentials_provider = oss.credentials.StaticCredentialsProvider(access_key_id, access_key_secret)

        # 配置客户端
        cfg = oss.config.load_default()
        cfg.credentials_provider = credentials_provider
        cfg.region = region
        cfg.endpoint = endpoint

        self._client = oss.Client(cfg)

    def _get_object_key(self, path: str) -> str:
        """将相对路径转换为 OSS 对象键"""
        if self._base_path:
            return f"{self._base_path}/{path}"
        return path

    def exists(self, path: str) -> bool:
        """检查对象是否存在"""
        key = self._get_object_key(path)
        return self._client.is_object_exist(bucket=self._bucket_name, key=key)

    def upload(self, path: str, dest: str) -> None:
        """上传文件或目录到 OSS"""
        try:
            key = self._get_object_key(dest)

            if os.path.isfile(path):
                # 上传单个文件
                with open(path, "rb") as f:
                    self._client.put_object(oss.PutObjectRequest(bucket=self._bucket_name, key=key, body=f))
            elif os.path.isdir(path):
                # 上传目录中的所有文件
                for root, _, files in os.walk(path):
                    for file in files:
                        local_file = os.path.join(root, file)
                        rel_path = os.path.relpath(local_file, path)
                        object_key = f"{key}/{rel_path}" if key else rel_path
                        with open(local_file, "rb") as f:
                            self._client.put_object(
                                oss.PutObjectRequest(bucket=self._bucket_name, key=object_key, body=f)
                            )
            else:
                raise CloudStorageError(f"路径不存在或不是文件/目录：{path}")
        except (oss.exceptions.OperationError, OSError) as e:
            raise CloudStorageError(f"上传文件失败：{path}", str(e)) from e

    def download(self, path: str, dest: str) -> None:
        """从 OSS 下载文件或目录"""
        try:
            key = self._get_object_key(path)

            # 检查是否是目录（通过列出对象判断）
            paginator = self._client.list_objects_v2_paginator()
            objects = []
            for page in paginator.iter_page(oss.ListObjectsV2Request(bucket=self._bucket_name, prefix=key, max_keys=2)):
                if page.contents:
                    objects.extend(page.contents)
                    if len(objects) >= 2:
                        break

            if not objects:
                raise CloudStorageError(f"OSS 中不存在对象：{path}")

            # 如果 key 以 / 结尾或有多个对象，视为目录
            is_directory = key.endswith("/") or len(objects) > 1

            if is_directory:
                # 下载目录
                os.makedirs(dest, exist_ok=True)
                paginator = self._client.list_objects_v2_paginator()
                for page in paginator.iter_page(oss.ListObjectsV2Request(bucket=self._bucket_name, prefix=key)):
                    if not page.contents:
                        continue
                    for obj in page.contents:
                        if not obj.key:
                            continue
                        # 计算本地路径
                        rel_path = obj.key[len(key) :].lstrip("/")
                        if not rel_path:
                            # 跳过目录对象本身
                            continue
                        local_file = os.path.join(dest, rel_path)
                        dir_path = os.path.dirname(local_file)
                        if dir_path:
                            os.makedirs(dir_path, exist_ok=True)
                        self._client.get_object_to_file(
                            oss.GetObjectRequest(bucket=self._bucket_name, key=obj.key),
                            local_file,
                        )
            else:
                # 下载单个文件
                dir_path = os.path.dirname(dest)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                self._client.get_object_to_file(oss.GetObjectRequest(bucket=self._bucket_name, key=key), dest)
        except oss.exceptions.OperationError as e:
            raise CloudStorageError(f"下载文件失败：{path}", str(e)) from e

    def read(self, path: str) -> str | None:
        """读取 OSS 文件内容"""
        try:
            key = self._get_object_key(path)
            result = self._client.get_object(oss.GetObjectRequest(bucket=self._bucket_name, key=key))
            if result.body:
                try:
                    content = result.body.content
                    return content.decode("utf-8") if content else ""
                finally:
                    result.body.close()
            return ""
        except oss.exceptions.OperationError as e:
            se = e.unwrap()
            if isinstance(se, oss.exceptions.ServiceError) and (
                se.code == "NoSuchKey" or (se.status_code == 404 and se.code == "BadErrorResponse")
            ):
                return None
            raise CloudStorageError(f"读取文件失败：{path}", str(e)) from e

    def remove(self, path: str, recursive=False) -> None:
        """删除 OSS 对象"""
        try:
            key = self._get_object_key(path)

            if recursive:
                # 递归删除所有匹配的对象
                keys_to_delete = []
                paginator = self._client.list_objects_v2_paginator()
                for page in paginator.iter_page(oss.ListObjectsV2Request(bucket=self._bucket_name, prefix=key)):
                    if page.contents:
                        keys_to_delete.extend([obj.key for obj in page.contents if obj.key])

                # 批量删除（每次最多 1000 个）
                for i in range(0, len(keys_to_delete), 1000):
                    batch = keys_to_delete[i : i + 1000]
                    objects = [oss.DeleteObject(key=k) for k in batch]
                    self._client.delete_multiple_objects(
                        oss.DeleteMultipleObjectsRequest(
                            bucket=self._bucket_name,
                            objects=objects,
                        )
                    )
            else:
                # 删除单个对象
                self._client.delete_object(oss.DeleteObjectRequest(bucket=self._bucket_name, key=key))
        except oss.exceptions.OperationError as e:
            raise CloudStorageError(f"删除文件失败：{path}", str(e)) from e

    def filter_files(self, path: str, include: list[str], exclude: list[str]) -> list[CloudFileMetadata]:
        """列出并过滤文件"""
        try:
            key = self._get_object_key(path)
            result = []

            paginator = self._client.list_objects_v2_paginator()
            for page in paginator.iter_page(oss.ListObjectsV2Request(bucket=self._bucket_name, prefix=key)):
                if not page.contents:
                    continue

                for obj in page.contents:
                    if not obj.key or obj.last_modified is None or obj.size is None:
                        continue

                    # 获取相对路径
                    rel_path = obj.key[len(self._base_path) + 1 :] if self._base_path else obj.key

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
                            modified_time=obj.last_modified,
                            size=obj.size,
                        )
                    )

            return result
        except oss.exceptions.OperationError as e:
            raise CloudStorageError(f"列出文件失败：{path}", str(e)) from e


class S3Client:
    def __init__(
        self,
        prefix: str,
        region: str,
        access_key_id: str,
        access_key_secret: str,
        endpoint: str,
    ):
        self._prefix = prefix

        if not prefix.startswith("s3://"):
            raise ValueError(f"S3 prefix 必须以 s3:// 开头：{prefix}")

        # 解析 bucket 和 base_path
        parts = prefix[5:].split("/", 1)
        self._bucket_name = parts[0]
        self._base_path = parts[1] if len(parts) > 1 else ""

        # 初始化 boto3 客户端
        try:
            if endpoint:
                # 使用自定义端点（MinIO 等）
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
            raise CloudStorageError("S3 认证失败", str(e)) from e

    def _get_object_key(self, path: str) -> str:
        """将相对路径转换为 S3 对象键"""
        if self._base_path:
            return f"{self._base_path}/{path}"
        return path

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
        """上传文件或目录到 S3"""
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
        """从 S3 下载文件或目录"""
        try:
            key = self._get_object_key(path)

            # 检查是否是目录（通过列出对象判断）
            response = self._client.list_objects_v2(Bucket=self._bucket_name, Prefix=key, MaxKeys=2)

            objects = response.get("Contents", [])
            if not objects:
                raise CloudStorageError(f"S3 中不存在对象：{path}")

            # 如果 key 以 / 结尾或有多个对象，视为目录
            is_directory = key.endswith("/") or len(objects) > 1

            if is_directory:
                # 下载目录
                os.makedirs(dest, exist_ok=True)
                paginator = self._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self._bucket_name, Prefix=key):
                    for obj in page.get("Contents", []):
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
        """读取 S3 文件内容"""
        try:
            key = self._get_object_key(path)
            response = self._client.get_object(Bucket=self._bucket_name, Key=key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise CloudStorageError(f"读取文件失败：{path}", str(e)) from e

    def remove(self, path: str, recursive=False) -> None:
        """删除 S3 对象"""
        try:
            key = self._get_object_key(path)

            if recursive:
                # 递归删除所有匹配的对象
                objects_to_delete = []
                paginator = self._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self._bucket_name, Prefix=key):
                    objects_to_delete.extend({"Key": obj["Key"]} for obj in page.get("Contents", []))

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

            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket_name, Prefix=key):
                for obj in page.get("Contents", []):
                    obj_key = obj["Key"]
                    # 获取相对路径
                    rel_path = obj_key[len(self._base_path) + 1 :] if self._base_path else obj_key

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


def create_cloud_client(
    prefix: str,
    region: str,
    access_key_id: str,
    access_key_secret: str,
    endpoint: str,
) -> OSSClient | S3Client:
    """
    根据 prefix 协议自动选择云存储客户端

    Args:
        prefix: 云存储路径，格式为 oss://bucket/path 或 s3://bucket/path
        region: AWS S3 区域（OSS 不使用）
        access_key_id: 访问密钥 ID
        access_key_secret: 访问密钥密文
        endpoint: 自定义端点（优先级高于 region）

    Returns:
        OSSClient 或 S3Client 实例

    Raises:
        ConfigError: 当 prefix 协议不支持时
    """
    if prefix.startswith("oss://"):
        return OSSClient(prefix, region, access_key_id, access_key_secret, endpoint)
    elif prefix.startswith("s3://"):
        return S3Client(prefix, region, access_key_id, access_key_secret, endpoint)
    else:
        raise ConfigError(f"不支持的云存储协议，prefix 必须以 oss:// 或 s3:// 开头：{prefix}")
