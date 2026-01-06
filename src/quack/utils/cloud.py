#!/usr/bin/env python3

import fnmatch
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List

import boto3
import oss2
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
        region: str,
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

        # 初始化 OSS 认证和 Bucket
        if not all([access_key_id, access_key_secret, endpoint]):
            raise ValueError(
                "使用 oss2 SDK 需要提供 access_key_id、access_key_secret 和 endpoint 参数"
            )

        self._auth = oss2.Auth(access_key_id, access_key_secret)
        self._bucket = oss2.Bucket(self._auth, endpoint, self._bucket_name)

    def _get_object_key(self, path: str) -> str:
        """将相对路径转换为 OSS 对象键"""
        if self._base_path:
            return f"{self._base_path}/{path}"
        return path

    def exists(self, path: str) -> bool:
        """检查对象是否存在"""
        try:
            key = self._get_object_key(path)
            self._bucket.head_object(key)
            return True
        except oss2.exceptions.NoSuchKey:
            return False
        except oss2.exceptions.OssError as e:
            raise CloudStorageError(f"检查文件是否存在失败：{path}", str(e))

    def upload(self, path: str, dest: str) -> None:
        """上传文件或目录到 OSS"""
        try:
            key = self._get_object_key(dest)

            if os.path.isfile(path):
                # 上传单个文件
                self._bucket.put_object_from_file(key, path)
            elif os.path.isdir(path):
                # 上传目录中的所有文件
                for root, _, files in os.walk(path):
                    for file in files:
                        local_file = os.path.join(root, file)
                        rel_path = os.path.relpath(local_file, path)
                        object_key = f"{key}/{rel_path}" if key else rel_path
                        self._bucket.put_object_from_file(object_key, local_file)
            else:
                raise CloudStorageError(f"路径不存在或不是文件/目录：{path}")
        except oss2.exceptions.OssError as e:
            raise CloudStorageError(f"上传文件失败：{path}", str(e))

    def download(self, path: str, dest: str) -> None:
        """从 OSS 下载文件或目录"""
        try:
            key = self._get_object_key(path)

            # 检查是否是目录（通过列出对象判断）
            objects = list(oss2.ObjectIterator(self._bucket, prefix=key, max_keys=2))

            if not objects:
                raise CloudStorageError(f"OSS 中不存在对象：{path}")

            # 如果 key 以 / 结尾或有多个对象，视为目录
            is_directory = key.endswith("/") or len(objects) > 1

            if is_directory:
                # 下载目录
                os.makedirs(dest, exist_ok=True)
                for obj in oss2.ObjectIterator(self._bucket, prefix=key):
                    # 计算本地路径
                    rel_path = obj.key[len(key) :].lstrip("/")
                    if not rel_path:
                        # 跳过目录对象本身
                        continue
                    local_file = os.path.join(dest, rel_path)
                    dir_path = os.path.dirname(local_file)
                    if dir_path:
                        os.makedirs(dir_path, exist_ok=True)
                    self._bucket.get_object_to_file(obj.key, local_file)
            else:
                # 下载单个文件
                dir_path = os.path.dirname(dest)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                self._bucket.get_object_to_file(key, dest)
        except oss2.exceptions.OssError as e:
            raise CloudStorageError(f"下载文件失败：{path}", str(e))

    def read(self, path: str) -> str | None:
        """读取 OSS 文件内容"""
        try:
            key = self._get_object_key(path)
            result = self._bucket.get_object(key)
            return result.read().decode("utf-8")
        except oss2.exceptions.NoSuchKey:
            return None
        except oss2.exceptions.OssError as e:
            raise CloudStorageError(f"读取文件失败：{path}", str(e))

    def remove(self, path: str, recursive=False) -> None:
        """删除 OSS 对象"""
        try:
            key = self._get_object_key(path)

            if recursive:
                # 递归删除所有匹配的对象
                keys_to_delete = []
                for obj in oss2.ObjectIterator(self._bucket, prefix=key):
                    keys_to_delete.append(obj.key)

                # 批量删除（每次最多 1000 个）
                for i in range(0, len(keys_to_delete), 1000):
                    batch = keys_to_delete[i : i + 1000]
                    self._bucket.batch_delete_objects(batch)
            else:
                # 删除单个对象
                self._bucket.delete_object(key)
        except oss2.exceptions.OssError as e:
            raise CloudStorageError(f"删除文件失败：{path}", str(e))

    def filter_files(
        self, path: str, include: List[str], exclude: List[str]
    ) -> List[CloudFileMetadata]:
        """列出并过滤文件"""
        try:
            key = self._get_object_key(path)
            result = []

            for obj in oss2.ObjectIterator(self._bucket, prefix=key):
                # 获取相对路径
                rel_path = (
                    obj.key[len(self._base_path) + 1 :] if self._base_path else obj.key
                )

                # 简单的模式匹配（支持通配符）
                if include:
                    match_include = any(
                        fnmatch.fnmatch(rel_path, pattern) for pattern in include
                    )
                    if not match_include:
                        continue

                if exclude:
                    match_exclude = any(
                        fnmatch.fnmatch(rel_path, pattern) for pattern in exclude
                    )
                    if match_exclude:
                        continue

                result.append(
                    CloudFileMetadata(
                        path=rel_path,
                        modified_time=datetime.fromtimestamp(obj.last_modified),
                        size=obj.size,
                    )
                )

            return result
        except oss2.exceptions.OssError as e:
            raise CloudStorageError(f"列出文件失败：{path}", str(e))


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
            raise CloudStorageError("S3 认证失败", str(e))

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
            raise CloudStorageError(f"检查文件是否存在失败：{path}", str(e))

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
                        self._client.upload_file(
                            local_file, self._bucket_name, object_key
                        )
            else:
                raise CloudStorageError(f"路径不存在或不是文件/目录：{path}")
        except ClientError as e:
            raise CloudStorageError(f"上传文件失败：{path}", str(e))

    def download(self, path: str, dest: str) -> None:
        """从 S3 下载文件或目录"""
        try:
            key = self._get_object_key(path)

            # 检查是否是目录（通过列出对象判断）
            response = self._client.list_objects_v2(
                Bucket=self._bucket_name, Prefix=key, MaxKeys=2
            )

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
                        self._client.download_file(
                            self._bucket_name, obj_key, local_file
                        )
            else:
                # 下载单个文件
                dir_path = os.path.dirname(dest)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                self._client.download_file(self._bucket_name, key, dest)
        except ClientError as e:
            raise CloudStorageError(f"下载文件失败：{path}", str(e))

    def read(self, path: str) -> str | None:
        """读取 S3 文件内容"""
        try:
            key = self._get_object_key(path)
            response = self._client.get_object(Bucket=self._bucket_name, Key=key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise CloudStorageError(f"读取文件失败：{path}", str(e))

    def remove(self, path: str, recursive=False) -> None:
        """删除 S3 对象"""
        try:
            key = self._get_object_key(path)

            if recursive:
                # 递归删除所有匹配的对象
                objects_to_delete = []
                paginator = self._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self._bucket_name, Prefix=key):
                    for obj in page.get("Contents", []):
                        objects_to_delete.append({"Key": obj["Key"]})

                # 批量删除（每次最多 1000 个）
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    self._client.delete_objects(
                        Bucket=self._bucket_name, Delete={"Objects": batch}
                    )
            else:
                # 删除单个对象
                self._client.delete_object(Bucket=self._bucket_name, Key=key)
        except ClientError as e:
            raise CloudStorageError(f"删除文件失败：{path}", str(e))

    def filter_files(
        self, path: str, include: List[str], exclude: List[str]
    ) -> List[CloudFileMetadata]:
        """列出并过滤文件"""
        try:
            key = self._get_object_key(path)
            result = []

            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket_name, Prefix=key):
                for obj in page.get("Contents", []):
                    obj_key = obj["Key"]
                    # 获取相对路径
                    rel_path = (
                        obj_key[len(self._base_path) + 1 :]
                        if self._base_path
                        else obj_key
                    )

                    # 简单的模式匹配（支持通配符）
                    if include:
                        match_include = any(
                            fnmatch.fnmatch(rel_path, pattern) for pattern in include
                        )
                        if not match_include:
                            continue

                    if exclude:
                        match_exclude = any(
                            fnmatch.fnmatch(rel_path, pattern) for pattern in exclude
                        )
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
            raise CloudStorageError(f"列出文件失败：{path}", str(e))


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
        raise ConfigError(
            f"不支持的云存储协议，prefix 必须以 oss:// 或 s3:// 开头：{prefix}"
        )
