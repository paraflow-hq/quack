#!/usr/bin/env python3

import fnmatch
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

import oss2


class OSSError(Exception):
    def __init__(self, message: str, details: str = ""):
        self.message = message
        self.details = details
        super().__init__(f"{message}: {details}" if details else message)


@dataclass
class OSSFileMetadata:
    path: str
    modified_time: datetime
    size: int


class OSSClient:
    def __init__(
        self,
        prefix: str,
        ak: Union[str, None] = None,
        sk: Union[str, None] = None,
        endpoint: Union[str, None] = None,
    ):
        self._prefix = prefix

        if not prefix.startswith("oss://"):
            raise ValueError(f"OSS prefix 必须以 oss:// 开头：{prefix}")

        parts = prefix[6:].split("/", 1)
        self._bucket_name = parts[0]
        self._base_path = parts[1] if len(parts) > 1 else ""

        # 初始化 OSS 认证和 Bucket
        if not all([ak, sk, endpoint]):
            raise ValueError("使用 oss2 SDK 需要提供 ak、sk 和 endpoint 参数")

        self._auth = oss2.Auth(ak, sk)
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
            raise OSSError(f"检查文件是否存在失败：{path}", str(e))

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
                raise OSSError(f"路径不存在或不是文件/目录：{path}")
        except oss2.exceptions.OssError as e:
            raise OSSError(f"上传文件失败：{path}", str(e))

    def download(self, path: str, dest: str) -> None:
        """从 OSS 下载文件或目录"""
        try:
            key = self._get_object_key(path)

            # 检查是否是目录（通过列出对象判断）
            objects = list(oss2.ObjectIterator(self._bucket, prefix=key, max_keys=2))

            if not objects:
                raise OSSError(f"OSS 中不存在对象：{path}")

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
            raise OSSError(f"下载文件失败：{path}", str(e))

    def read(self, path: str) -> Union[str, None]:
        """读取 OSS 文件内容"""
        try:
            key = self._get_object_key(path)
            result = self._bucket.get_object(key)
            return result.read().decode("utf-8")
        except oss2.exceptions.NoSuchKey:
            return None
        except oss2.exceptions.OssError as e:
            raise OSSError(f"读取文件失败：{path}", str(e))

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
            raise OSSError(f"删除文件失败：{path}", str(e))

    def filter_files(
        self, path: str, include: List[str], exclude: List[str]
    ) -> List[OSSFileMetadata]:
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
                    OSSFileMetadata(
                        path=rel_path,
                        modified_time=datetime.fromtimestamp(obj.last_modified),
                        size=obj.size,
                    )
                )

            return result
        except oss2.exceptions.OssError as e:
            raise OSSError(f"列出文件失败：{path}", str(e))
