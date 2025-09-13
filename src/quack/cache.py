#!/usr/bin/env python3

from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta
from typing import final, override

from loguru import logger
from xdg_base_dirs import xdg_cache_home

from quack.config import Config
from quack.consts import CACHE_METADATA_FILENAME, SERVE_BASE_PATH
from quack.exceptions import ChecksumError
from quack.models.target import Target
from quack.utils.archiver import Archiver
from quack.utils.ci_environment import CIEnvironment
from quack.utils.metadata import Metadata
from quack.utils.oss import OSSClient


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
        logger.info(f"正在从本地加载 Target {target.name} 的缓存...")
        Archiver.extract(self.get_archive_path(target))

    def save(self, target: Target) -> None:
        os.makedirs(self.get_cache_path(target), exist_ok=True)

        archive_path = self.get_archive_path(target)
        logger.debug(f"正在保存缓存到本地路径 {archive_path}...")

        Archiver.archive(target.outputs.paths, archive_path)
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
                    need_clear = (
                        datetime.now() - last_cleared
                    ).days > self.CACHE_CLEAR_DURATION_DAYS
            except FileNotFoundError:
                need_clear = True

        if need_clear:
            logger.info("清理过期缓存...")
            for root, dirs, _ in os.walk(self._cache_base_path):
                for d in dirs:
                    # 忽略 Target 以外的目录
                    if ":" not in d:
                        continue
                    full_path = os.path.join(root, d)
                    atime = datetime.fromtimestamp(os.path.getatime(full_path))
                    if (datetime.now() - atime).days > self.CACHE_EXPIRE_DAYS:
                        logger.debug(f"正在清理过期缓存 {full_path}...")
                        shutil.rmtree(full_path)

            with open(last_cleared_path, "w") as f:
                _ = f.write(datetime.now().isoformat())


class TargetCacheBackendTypeOSS:
    NAME: str = "oss"

    CACHE_EXPIRE_DAYS: int = 15

    def __init__(self, config: Config, app_name: str) -> None:
        self._config: Config = config
        self._app_name: str = app_name
        self._cache_base_path: str = os.path.join(".quack-cache", app_name)
        self._local_backend: TargetCacheBackendTypeLocal | None = None
        self._oss_client: OSSClient | None = None

    @property
    def local_backend(self) -> TargetCacheBackendTypeLocal:
        if self._local_backend is None:
            self._local_backend = TargetCacheBackendTypeLocal(
                self._config, self._app_name
            )
        return self._local_backend

    @property
    def oss_client(self) -> OSSClient:
        if self._oss_client is None:
            self._oss_client = OSSClient(
                prefix=self._config.oss.prefix,
                ak=self._config.oss.access_key_id,
                sk=self._config.oss.access_key_secret,
                endpoint=self._config.oss.endpoint,
                log_level="info",
                parallel_level=50,
            )
        return self._oss_client

    def get_cache_path(self, target: Target) -> str:
        return f"{self._cache_base_path}/{target.cache_path}"

    def get_archive_path(self, target: Target) -> str:
        return f"{self.get_cache_path(target)}/{target.cache_archive_filename}"

    def get_metadata_path(self, target: Target) -> str:
        return f"{self.get_cache_path(target)}/{CACHE_METADATA_FILENAME}"

    def get_commits_path(self) -> str:
        commit_sha = CIEnvironment().commit_sha
        if commit_sha:
            return os.path.join(self._cache_base_path, "_commits", commit_sha)
        else:
            return ""

    def get_commit_metadata_path(self, target: Target) -> str:
        return os.path.join(self.get_commits_path(), f"{target.name}.json")

    def exists(self, target: Target) -> bool:
        return self.oss_client.exists(self.get_metadata_path(target))

    def update_access_time(self, target: Target) -> None:
        """重新上传一次 metadata 文件，来标识其被访问过"""
        self.oss_client.upload(
            self.local_backend.get_metadata_path(target),
            self.get_metadata_path(target),
            force=True,
        )

    def load(self, target: Target, update_access_time: bool = True) -> None:
        if self.local_backend.exists(target):
            try:
                self.local_backend.load(target)
                if update_access_time:
                    self.update_access_time(target)
                return
            except ChecksumError:
                logger.warning("本地缓存已损坏，从 OSS 重新下载")

        logger.info(f"正在从 OSS 加载 Target {target.name} 的缓存...")
        self.oss_client.download(
            self.get_archive_path(target), self.local_backend.get_archive_path(target)
        )
        self.oss_client.download(
            self.get_metadata_path(target), self.local_backend.get_metadata_path(target)
        )
        self.local_backend.load(target)
        if update_access_time:
            self.update_access_time(target)

    def save(self, target: Target) -> None:
        self.local_backend.save(target)
        archive_path = self.get_archive_path(target)
        logger.debug(f"正在上传缓存到 OSS 路径 {archive_path}...")
        self.oss_client.upload(
            self.local_backend.get_archive_path(target), archive_path
        )
        self.oss_client.upload(
            self.local_backend.get_metadata_path(target), self.get_metadata_path(target)
        )

        # 记录成功执行的 target metadata，方便根据 commit sha 进行 load
        if self._config.save_for_load and self.get_commits_path():
            self.oss_client.upload(
                self.local_backend.get_metadata_path(target),
                self.get_commit_metadata_path(target),
            )

    def clear_expired(self) -> None:
        logger.info("清理过期缓存...")
        file_metadatas = self.oss_client.filter_files(
            self._cache_base_path,
            include=[CACHE_METADATA_FILENAME],
            exclude=[],
        )
        for m in file_metadatas:
            if datetime.now() - m.modified_time > timedelta(
                days=self.CACHE_EXPIRE_DAYS
            ):
                cache_dir = m.path[: -len(CACHE_METADATA_FILENAME)]
                assert cache_dir.startswith(self._cache_base_path)
                logger.info(f"正在清理过期缓存 {cache_dir}...")
                self.oss_client.remove(cache_dir, recursive=True)


class TargetCacheBackendTypeDev(TargetCacheBackendTypeOSS):
    """本地开发使用该 Backend，使用专用 OSS 目录，但获取缓存时会先尝试从 CI OSS 加载"""

    NAME: str = "dev"

    CACHE_EXPIRE_DAYS: int = 3

    def __init__(self, config: Config, app_name: str) -> None:
        super().__init__(config, app_name)
        self._cache_base_path: str = os.path.join(".quack-cache-dev", app_name)
        self._ci_oss_backend: TargetCacheBackendTypeOSS = TargetCacheBackendTypeOSS(
            config, app_name
        )

    @override
    def exists(self, target: Target) -> bool:
        if self._ci_oss_backend.exists(target):
            return True
        else:
            return super().exists(target)

    @override
    def load(self, target: Target, update_access_time: bool = True) -> None:
        if self._ci_oss_backend.exists(target):
            self._ci_oss_backend.load(target, update_access_time=False)
        else:
            super().load(target, update_access_time)


class TargetCacheBackendTypeServe(TargetCacheBackendTypeDev):
    """使用 remote 模式时，开发机端使用该 Backend，除了正常执行之外，还会将产物保存到临时目录，以便 rsync 回传"""

    NAME: str = "serve"

    def __init__(self, config: Config, app_name: str) -> None:
        super().__init__(config, app_name)
        os.makedirs(SERVE_BASE_PATH, exist_ok=True)

    @override
    def load(self, target: Target, update_access_time: bool = True) -> None:
        super().load(target, update_access_time)
        Archiver.extract(self.local_backend.get_archive_path(target), SERVE_BASE_PATH)

    @override
    def save(self, target: Target) -> None:
        super().save(target)
        Archiver.extract(self.local_backend.get_archive_path(target), SERVE_BASE_PATH)


TargetCacheBackendType = (
    TargetCacheBackendTypeRaw
    | TargetCacheBackendTypeLocal
    | TargetCacheBackendTypeOSS
    | TargetCacheBackendTypeDev
    | TargetCacheBackendTypeServe
)

TargetCacheBackendTypeMap: dict[str, type[TargetCacheBackendType]] = {
    backend.NAME: backend
    for backend in [
        TargetCacheBackendTypeRaw,
        TargetCacheBackendTypeLocal,
        TargetCacheBackendTypeOSS,
        TargetCacheBackendTypeDev,
        TargetCacheBackendTypeServe,
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
