#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import final, override

import zstandard as zstd
from loguru import logger
from xdg_base_dirs import xdg_cache_home

from quack.config import Config
from quack.consts import CACHE_METADATA_FILENAME
from quack.exceptions import CacheCorruptionError, CloudStorageTransientError
from quack.models.target import Target
from quack.utils.archiver import Archiver
from quack.utils.ci_environment import CIEnvironment
from quack.utils.cloud import CloudClient
from quack.utils.formatter import format_size
from quack.utils.metadata import Metadata


class TargetCacheBackendTypeRaw:
    NAME: str = "false"

    def __init__(self, config: Config, app_name: str):
        pass

    def exists(self, _: Target) -> bool:
        return False

    def load(self, _: Target) -> None:
        pass

    def save(self, _: Target) -> None:
        pass


class TargetCacheBackendTypeLocal:
    NAME: str = "local"

    CACHE_CLEAR_DURATION_DAYS: int = 7
    CACHE_EXPIRE_DAYS: int = 15

    def __init__(self, _config: Config, app_name: str):
        self._app_name: str = app_name

        self._cache_base_path: str = os.path.join(xdg_cache_home(), "quack", app_name)

    def get_cache_path(self, target: Target) -> str:
        return os.path.join(self._cache_base_path, target.cache_path)

    def get_archive_path(self, target: Target) -> str:
        return os.path.join(self.get_cache_path(target), target.cache_archive_filename)

    def get_metadata_path(self, target: Target) -> str:
        return os.path.join(self.get_cache_path(target), CACHE_METADATA_FILENAME)

    def exists(self, target: Target) -> bool:
        return os.path.exists(self.get_metadata_path(target))

    def load(self, target: Target) -> None:
        # ossutil 自带 crc64 校验，不再需要 checksum，metadata 文件仅作为最后访问时间的标识
        # Checksummer.verify(self.get_archive_path(target), self.get_metadata_path(target))
        archive_path = self.get_archive_path(target)
        size = os.path.getsize(archive_path)
        logger.info(f"正在从本地加载 Target {target.name} 的缓存（大小：{format_size(size)}）...")
        try:
            Archiver.extract(archive_path)
        except zstd.ZstdError as e:
            raise CacheCorruptionError(f"缓存归档解压失败：{archive_path}") from e
        metadata_path = self.get_metadata_path(target)
        if os.path.exists(metadata_path):
            os.utime(metadata_path, None)

    def save(self, target: Target) -> None:
        os.makedirs(self.get_cache_path(target), exist_ok=True)

        archive_path = self.get_archive_path(target)
        logger.debug(f"正在保存缓存到本地路径 {archive_path}...")

        Archiver.archive(target.outputs.paths, archive_path)
        size = os.path.getsize(archive_path)
        logger.info(f"已生成缓存（大小：{format_size(size)}）")
        Metadata.generate(
            self.get_archive_path(target),
            self.get_metadata_path(target),
            target_checksum=target.checksum_value,
            commit_sha=CIEnvironment().commit_sha,
        )
        # Checksummer.generate(
        #     self.get_archive_path(target), self.get_metadata_path(target)
        # )

    def clear_expired(self) -> None:
        last_cleared_path = os.path.join(self._cache_base_path, "last_cleared")
        need_clear = False
        if os.path.isdir(self._cache_base_path):
            try:
                with open(last_cleared_path) as f:
                    iso_datetime = f.read().strip()
                    last_cleared = datetime.fromisoformat(iso_datetime)
                    need_clear = (datetime.now() - last_cleared).days >= self.CACHE_CLEAR_DURATION_DAYS
            except FileNotFoundError:
                need_clear = True

        if need_clear:
            logger.info("清理过期缓存...")
            expire_before = datetime.now() - timedelta(days=self.CACHE_EXPIRE_DAYS)
            # 缓存目录结构：<target>/<checksum[:2]>/<checksum[2:]>/
            # 只清理最底层的 checksum 目录，避免误删仍有有效缓存的父目录
            for target_dir in Path(self._cache_base_path).iterdir():
                if not target_dir.is_dir():
                    continue
                for prefix_dir in target_dir.iterdir():
                    if not prefix_dir.is_dir():
                        continue
                    for checksum_dir in prefix_dir.iterdir():
                        if not checksum_dir.is_dir():
                            continue
                        # 用 metadata 文件的 mtime 作为最后使用时间；
                        # 旧版本缓存没有 metadata 文件，则用目录 mtime 兜底
                        metadata_path = checksum_dir / CACHE_METADATA_FILENAME
                        if metadata_path.exists():
                            last_used = datetime.fromtimestamp(metadata_path.stat().st_mtime)
                        else:
                            last_used = datetime.fromtimestamp(checksum_dir.stat().st_mtime)
                        if last_used < expire_before:
                            logger.debug(f"清理过期缓存目录：{checksum_dir}")
                            shutil.rmtree(checksum_dir, ignore_errors=True)
                    # 清理已变空的父目录
                    with contextlib.suppress(OSError):
                        prefix_dir.rmdir()
                with contextlib.suppress(OSError):
                    target_dir.rmdir()

            with open(last_cleared_path, "w") as f:
                _ = f.write(datetime.now().isoformat())


class TargetCacheBackendTypeCloud:
    NAME: str = "cloud"

    CACHE_EXPIRE_DAYS: int = 15

    def __init__(self, config: Config, app_name: str) -> None:
        self._config: Config = config
        self._app_name: str = app_name
        self._cache_base_path: str = os.path.join(".quack-cache", app_name)
        self._local_backend: TargetCacheBackendTypeLocal | None = None
        self._cloud_client = None

    @property
    def local_backend(self) -> TargetCacheBackendTypeLocal:
        if self._local_backend is None:
            self._local_backend = TargetCacheBackendTypeLocal(self._config, self._app_name)
        return self._local_backend

    @property
    def cloud_client(self):
        if self._cloud_client is None:
            self._cloud_client = CloudClient(
                prefix=self._config.cloud.prefix,
                region=self._config.cloud.region,
                access_key_id=self._config.cloud.access_key_id,
                access_key_secret=self._config.cloud.access_key_secret,
                endpoint=self._config.cloud.endpoint,
            )
        return self._cloud_client

    def get_cache_path(self, target: Target) -> str:
        return f"{self._cache_base_path}/{target.cache_path}"

    def get_archive_path(self, target: Target) -> str:
        return f"{self.get_cache_path(target)}/{target.cache_archive_filename}"

    def get_metadata_path(self, target: Target) -> str:
        return f"{self.get_cache_path(target)}/{CACHE_METADATA_FILENAME}"

    def get_commit_path(self) -> str:
        commit_sha = CIEnvironment().commit_sha
        if commit_sha:
            return os.path.join(self._cache_base_path, "_commits", commit_sha)
        else:
            return ""

    def get_commit_metadata_path(self, target: Target) -> str:
        return os.path.join(self.get_commit_path(), f"{target.name}.json")

    def exists(self, target: Target) -> bool:
        return self.cloud_client.exists(self.get_metadata_path(target))

    def update_access_time(self, target: Target) -> None:
        """重新上传一次 metadata 文件，来标识其被访问过"""
        try:
            self.cloud_client.upload(
                self.local_backend.get_metadata_path(target),
                self.get_metadata_path(target),
            )
        except CloudStorageTransientError as e:
            logger.warning(f"更新缓存访问时间失败，将跳过：{e}")

    def load(self, target: Target, update_access_time: bool = True) -> None:
        if self.local_backend.exists(target):
            try:
                self.local_backend.load(target)
                if update_access_time:
                    self.update_access_time(target)
                return
            except CacheCorruptionError:
                logger.warning("本地缓存已损坏，从云存储重新下载")
                shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)

        try:
            logger.info(f"正在从云存储加载 Target {target.name} 的缓存...")
            self.cloud_client.download(self.get_archive_path(target), self.local_backend.get_archive_path(target))
            self.cloud_client.download(self.get_metadata_path(target), self.local_backend.get_metadata_path(target))
            self.local_backend.load(target)
        except CacheCorruptionError:
            logger.warning(f"云存储中 Target {target.name} 的缓存已损坏，将重新生成")
            shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)
            raise
        except CloudStorageTransientError:
            shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)
            raise
        if update_access_time:
            self.update_access_time(target)

    def save(self, target: Target) -> None:
        self.local_backend.save(target)
        archive_path = self.get_archive_path(target)
        logger.debug(f"正在上传缓存到云存储路径 {archive_path}...")
        self.cloud_client.upload(self.local_backend.get_archive_path(target), archive_path)
        self.cloud_client.upload(self.local_backend.get_metadata_path(target), self.get_metadata_path(target))

    def clear_expired(self) -> None:
        logger.info("清理过期缓存...")
        file_metadatas = self.cloud_client.filter_files(
            self._cache_base_path,
            include=[CACHE_METADATA_FILENAME],
            exclude=[],
        )
        for m in file_metadatas:
            if datetime.now() - m.modified_time > timedelta(days=self.CACHE_EXPIRE_DAYS):
                cache_dir = m.path[: -len(CACHE_METADATA_FILENAME)]
                assert cache_dir.startswith(self._cache_base_path)
                logger.info(f"正在清理过期缓存 {cache_dir}...")
                self.cloud_client.remove(cache_dir, recursive=True)


class TargetCacheBackendTypeDev(TargetCacheBackendTypeCloud):
    """本地开发使用该 Backend，使用专用云存储目录，但获取缓存时会先尝试从 CI 云存储加载"""

    NAME: str = "dev"

    CACHE_EXPIRE_DAYS: int = 3

    def __init__(self, config: Config, app_name: str) -> None:
        super().__init__(config, app_name)
        self._cache_base_path: str = os.path.join(".quack-cache-dev", app_name)
        self._ci_cloud_backend: TargetCacheBackendTypeCloud = TargetCacheBackendTypeCloud(config, app_name)

    @override
    def exists(self, target: Target) -> bool:
        if self._ci_cloud_backend.exists(target):
            return True
        else:
            return super().exists(target)

    @override
    def load(self, target: Target, update_access_time: bool = True) -> None:
        if self._ci_cloud_backend.exists(target):
            self._ci_cloud_backend.load(target, update_access_time=False)
        else:
            super().load(target, update_access_time)


TargetCacheBackendType = (
    TargetCacheBackendTypeRaw | TargetCacheBackendTypeLocal | TargetCacheBackendTypeCloud | TargetCacheBackendTypeDev
)

TargetCacheBackendTypeMap: dict[str, type[TargetCacheBackendType]] = {
    backend.NAME: backend
    for backend in [
        TargetCacheBackendTypeRaw,
        TargetCacheBackendTypeLocal,
        TargetCacheBackendTypeCloud,
        TargetCacheBackendTypeDev,
    ]
}


@final
class TargetCache:
    def __init__(
        self,
        config: Config,
        app_name: str,
        target: Target,
        backend_type: type[TargetCacheBackendType],
    ) -> None:
        self._config = config
        self._app_name = app_name
        self.target = target
        self._backend_type = backend_type
        self._backend = None

    @property
    def backend(self) -> TargetCacheBackendType:
        if self._backend is None:
            self._backend = self._backend_type(self._config, self._app_name)
        return self._backend

    def hit(self) -> bool:
        return self.backend.exists(self.target)

    def miss(self) -> bool:
        return not self.hit()

    def load(self) -> None:
        self.backend.load(self.target)

    def save(self) -> None:
        self.backend.save(self.target)
