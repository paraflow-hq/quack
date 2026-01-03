#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import sys
from enum import Enum
from typing import TYPE_CHECKING

from loguru import logger
from pydantic import Field

from quack.config import Config
from quack.models.base import BaseModel
from quack.models.command import Command
from quack.models.dependency import Dependency, DependencyTypeTarget
from quack.utils.oss import OSSError

if TYPE_CHECKING:
    from quack.cache import TargetCacheBackendType


class TargetExecutionMode(Enum):
    NORMAL = "normal"  # 尝试加载，不存在则重新构建
    DEPS_ONLY = "deps-only"  # 类似 normal，但仅构建依赖项
    LOAD_ONLY = "load-only"  # 仅尝试加载缓存，不进行构建


class TargetOutputs(BaseModel):
    paths: set[str] = Field(default_factory=set)
    inherit: bool = Field(default=False)


class TargetOperations(BaseModel):
    build: Command


class Target(BaseModel):
    name: str = Field(max_length=48, pattern=r"^[a-z0-9\-]+:[a-z0-9\-:]+$")
    description: str = Field(max_length=255)
    dependencies: list[Dependency] = Field(default_factory=list)
    outputs: TargetOutputs
    operations: TargetOperations

    _checksum_value: str | None = None

    @property
    def checksum_value(self) -> str:
        if self._checksum_value is None:
            self._checksum_value = self.compute_checksum()
        return self._checksum_value

    @checksum_value.setter
    def checksum_value(self, value: str) -> None:
        self._checksum_value = value

    @property
    def cache_path(self) -> str:
        return f"{self.name}/{self.checksum_value[:2]}/{self.checksum_value[2:]}"

    @property
    def cache_archive_filename(self) -> str:
        return f"{self.name}.tar.gz"

    def compute_checksum(self) -> str:
        hash_tuple = [dep.checksum_value for dep in self.dependencies]
        logger.debug(f"Target {self.name} 各依赖 Checksum 值：")
        for dep in self.dependencies:
            logger.debug(f"- {dep.display_name}: {dep.checksum_value}")
        return hashlib.sha256(repr(hash_tuple).encode("utf-8")).hexdigest()

    def prepare_deps(
        self, config: Config, app_name: str, cache_backend: type[TargetCacheBackendType]
    ) -> None:
        for dep in self.dependencies:
            if isinstance(dep, DependencyTypeTarget):
                dep.target.execute(config, app_name, cache_backend)

    def execute(
        self,
        config: Config,
        app_name: str,
        cache_backend: type[TargetCacheBackendType],
        mode: TargetExecutionMode = TargetExecutionMode.NORMAL,
    ) -> None:
        from quack.cache import TargetCache

        logger.info(f"正在执行 Target {self.name}...")
        logger.info(f"Target {self.name} Checksum 值：{self.checksum_value}")
        logger.info(f"正在查找 Target {self.name} 的缓存...")

        cache = TargetCache(config, app_name, self, cache_backend)
        cache_exists = cache.hit()

        if mode == TargetExecutionMode.DEPS_ONLY:
            self.prepare_deps(config, app_name, cache_backend)
        elif mode == TargetExecutionMode.LOAD_ONLY:
            if cache_exists:
                logger.info("找到缓存，直接从缓存加载...")
                cache.load()
            else:
                logger.error("未找到缓存，无法进行加载")
                sys.exit(1)
        else:
            if not cache_exists:
                logger.info(
                    f"未找到对应的缓存，开始重新生成缓存：{self.operations.build.command}"
                )
                self.prepare_deps(config, app_name, cache_backend)
                self.operations.build.execute()

            if cache_exists:
                logger.info("找到缓存，直接从缓存加载...")
                cache.load()
            else:
                logger.info(f"正在存入缓存，路径：{self.cache_path}")
                try:
                    cache.save()
                except OSSError as e:
                    logger.error(f"存入缓存失败：{e}")
                    sys.exit(1)

        logger.success("执行完毕！")
