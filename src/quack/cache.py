#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import final, override

import zstandard as zstd
from loguru import logger
from xdg_base_dirs import xdg_cache_home

from quack.config import CacheAccess, Config
from quack.consts import CACHE_METADATA_FILENAME
from quack.exceptions import (
    CacheCorruptionError,
    CacheSecurityError,
    CloudStorageError,
    CloudStorageTransientError,
    ConfigError,
)
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

    def __init__(self, config: Config, app_name: str):
        self._app_name: str = app_name
        self._config = config
        identity = [
            config.cloud.endpoint.rstrip("/").lower(),
            config.cloud.prefix.rstrip("/"),
            config.cloud.region,
            config.cloud.provider,
            config.cache_namespace,
            config.cache,
            2,
        ]
        namespace = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
        self._cache_base_path = os.path.join(xdg_cache_home(), "quack", "v2", namespace, app_name)

    def get_cache_path(self, target: Target) -> str:
        return os.path.join(self._cache_base_path, target.cache_path)

    def get_archive_path(self, target: Target) -> str:
        return os.path.join(self.get_cache_path(target), target.cache_archive_filename)

    def get_metadata_path(self, target: Target) -> str:
        return os.path.join(self.get_cache_path(target), CACHE_METADATA_FILENAME)

    def exists(self, target: Target) -> bool:
        return os.path.isfile(self.get_metadata_path(target)) and os.path.isfile(self.get_archive_path(target))

    def read_metadata(self, target: Target) -> Metadata:
        metadata = Metadata.read(self.get_metadata_path(target))
        if metadata.target_name != target.name or metadata.target_checksum != target.checksum_value:
            raise CacheSecurityError("Cache metadata does not match the requested target")
        if target.expected_archive_checksum is not None and metadata.file_checksum != target.expected_archive_checksum:
            raise CacheSecurityError("Cache archive does not match its commit index")
        return metadata

    def load(self, target: Target) -> None:
        archive_path = self.get_archive_path(target)
        metadata = self.read_metadata(target)
        size = os.path.getsize(archive_path)
        logger.info(f"正在从本地加载 Target {target.name} 的缓存（大小：{format_size(size)}）...")
        try:
            Archiver.extract(
                archive_path,
                outputs=target.outputs.paths,
                expected_checksum=metadata.file_checksum,
                limits=self._config.cache_limits,
            )
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
            target_name=target.name,
        )

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
                        # 保存中断导致 metadata 缺失时，用目录 mtime 兜底
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
        self._cache_base_path: str = os.path.join(".quack-cache-v2", app_name)
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
            if self._config.cache_access == CacheAccess.NONE:
                raise ConfigError("Remote cache access is disabled")
            self._cloud_client = CloudClient(self._config.cloud, self._config.cache_limits)
        return self._cloud_client

    def get_cache_path(self, target: Target) -> str:
        return f"{self._cache_base_path}/{target.cache_path}"

    def get_archive_path(self, target: Target, metadata: Metadata) -> str:
        if target.expected_archive_checksum is not None:
            # Format v2 commit indexes predate content addressing and name the target-scoped archive.
            return f"{self.get_cache_path(target)}/{target.cache_archive_filename}"
        # Producers share the checksum directory, so each archive gets its own digest-named object.
        return f"{self.get_cache_path(target)}/{metadata.file_checksum}.tar.zst"

    def get_metadata_path(self, target: Target) -> str:
        return f"{self.get_cache_path(target)}/{CACHE_METADATA_FILENAME}"

    def get_commit_metadata_path(self, target: Target, commit_sha: str) -> str:
        if len(commit_sha) != 40 or any(c not in "0123456789abcdef" for c in commit_sha):
            raise ConfigError("A full commit SHA is required for cache indexes")
        return f"{self._cache_base_path}/_commits/{commit_sha}/{target.name}.json"

    def _commit_archive_path(self, index: Metadata) -> str:
        return f"{self._cache_base_path}/_commit-archives/{index.file_checksum}.tar.zst"

    def publish_commit(self, target: Target, commit_sha: str) -> None:
        index_path = self.get_commit_metadata_path(target, commit_sha)
        with TemporaryDirectory() as staging:
            archive = str(Path(staging) / "archive.tar.zst")
            # Shared local cache files can change between this job's build and publication.
            Archiver.archive(target.outputs.paths, archive)
            with open(archive, "rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            index = Metadata(
                format_version=3,
                target_name=target.name,
                target_checksum=target.checksum_value,
                commit_sha=commit_sha,
                file_checksum=digest,
            )
            archive_path = self._commit_archive_path(index)
            if not self.cloud_client.exists(archive_path):
                self.cloud_client.upload(archive, archive_path)
            index_file = Path(staging) / "index.json"
            _ = index_file.write_text(index.model_dump_json(), encoding="utf-8")
            self.cloud_client.upload(str(index_file), index_path)

    def restore_commit(self, target: Target, index: Metadata) -> None:
        with TemporaryDirectory() as staging:
            archive = str(Path(staging) / "archive.tar.zst")
            self.cloud_client.download(self._commit_archive_path(index), archive)
            Archiver.extract(
                archive,
                outputs=target.outputs.paths,
                expected_checksum=index.file_checksum,
                limits=self._config.cache_limits,
            )

    def exists(self, target: Target) -> bool:
        return self.cloud_client.exists(self.get_metadata_path(target))

    def update_access_time(self, target: Target) -> None:
        """重新上传一次 metadata 文件，来标识其被访问过"""
        if self._config.cache_access != CacheAccess.READ_WRITE:
            return
        try:
            metadata = self.local_backend.read_metadata(target)
            # A local copy may come from a producer whose archive never reached this storage domain.
            if not self.cloud_client.exists(self.get_archive_path(target, metadata)):
                return
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

        # Metadata now lands first; a stale local archive must not pair with it if the archive download fails.
        shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)
        try:
            logger.info(f"正在从云存储加载 Target {target.name} 的缓存...")
            # Metadata names the exact archive, so a concurrent producer cannot swap the bytes between the two reads.
            self.cloud_client.download(
                self.get_metadata_path(target), self.local_backend.get_metadata_path(target), max_bytes=1024**2
            )
            metadata = self.local_backend.read_metadata(target)
            self.cloud_client.download(
                self.get_archive_path(target, metadata), self.local_backend.get_archive_path(target)
            )
            self.local_backend.load(target)
        except CloudStorageTransientError:
            shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)
            raise
        except CloudStorageError as error:
            if error.code in ("404", "NoSuchKey", "NotFound"):
                shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)
                raise CacheCorruptionError("Cache archive or metadata is missing") from error
            raise
        except CacheCorruptionError:
            logger.warning(f"云存储中 Target {target.name} 的缓存已损坏，将重新生成")
            shutil.rmtree(self.local_backend.get_cache_path(target), ignore_errors=True)
            raise
        if update_access_time:
            self.update_access_time(target)

    def save(self, target: Target) -> None:
        self.local_backend.save(target)
        if self._config.cache_access != CacheAccess.READ_WRITE:
            return
        archive_path = self.get_archive_path(target, self.local_backend.read_metadata(target))
        logger.debug(f"正在上传缓存到云存储路径 {archive_path}...")
        # Publish the archive before the metadata that names it.
        self.cloud_client.upload(self.local_backend.get_archive_path(target), archive_path)
        self.cloud_client.upload(self.local_backend.get_metadata_path(target), self.get_metadata_path(target))

    def clear_expired(self) -> None:
        if self._config.cache_access != CacheAccess.READ_WRITE:
            raise ConfigError("Clearing remote cache requires read_write access")
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
    """优先读取同一存储根的 cloud 缓存前缀，未命中时读取 dev 前缀。"""

    NAME: str = "dev"

    CACHE_EXPIRE_DAYS: int = 3

    def __init__(self, config: Config, app_name: str) -> None:
        super().__init__(config, app_name)
        self._cache_base_path: str = os.path.join(".quack-cache-dev-v2", app_name)
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
            backend_type = (
                TargetCacheBackendTypeRaw if self._config.cache_access == CacheAccess.NONE else self._backend_type
            )
            self._backend = backend_type(self._config, self._app_name)
        return self._backend

    def hit(self) -> bool:
        return self.backend.exists(self.target)

    def miss(self) -> bool:
        return not self.hit()

    def load(self) -> None:
        self.backend.load(self.target)

    def save(self) -> None:
        self.backend.save(self.target)
