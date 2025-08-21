#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import re
import sys
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from loguru import logger

from quack.config import Config
from quack.db import DB
from quack.exceptions import DBNotFoundError, SpecError
from quack.models.command import Command
from quack.models.dependency import (
    DependencyType,
    DependencyTypeCommand,
    DependencyTypeSource,
    DependencyTypeTarget,
    DependencyTypeVariable,
)
from quack.utils.ci_environment import CIEnvironment
from quack.utils.oss import OSSError

if TYPE_CHECKING:
    from quack.cache import TargetCacheBackendType


class TargetExecutionMode(Enum):
    NORMAL = "normal"  # 尝试加载，不存在则重新构建
    DEPS_ONLY = "deps-only"  # 类似 normal，但仅构建依赖项
    LOAD_ONLY = "load-only"  # 仅尝试加载缓存，不进行构建


@dataclass
class TargetOutputs:
    paths: set[str]

    def __init__(self, data) -> None:
        self.paths = set(data["paths"])

    def validate(self) -> None:
        pass


@dataclass
class TargetOperations:
    build: Command

    def __init__(self, data) -> None:
        self.build = Command(data["build"])

    def validate(self) -> None:
        self.build.validate()


@dataclass
class Target:
    name: str
    description: str
    dependencies: list[DependencyType]
    operations: TargetOperations

    def __init__(self, data) -> None:
        from quack.spec import Spec

        self.name = data["name"]
        self.description = data["description"]
        # 默认依赖于所有 quack.yaml
        self.dependencies = []
        spec = Spec.get()
        deps = data.get("dependencies", [])
        deps.insert(0, {"type": "global", "name": "source:quack"})
        for dep in deps.copy():
            index = deps.index(dep)
            if dep["type"] == "global":
                deps.remove(dep)
                try:
                    deps.insert(index, spec.global_dependencies[dep["name"]])
                except KeyError:
                    raise SpecError(
                        f"Target {self.name} 拥有未知全局依赖 {dep['name']}"
                    )
        for dep in deps:
            if dep["type"] == "command":
                self.dependencies.append(DependencyTypeCommand(dep))
            elif dep["type"] == "source":
                self.dependencies.append(DependencyTypeSource(dep))
            elif dep["type"] == "target":
                self.dependencies.append(DependencyTypeTarget(dep))
            elif dep["type"] == "variable":
                self.dependencies.append(DependencyTypeVariable(dep))
            else:
                raise SpecError(f"Target {self.name} 拥有未知类型 {dep['type']}")

        self.operations = TargetOperations(data["operations"])
        self._output_spec = data["outputs"]
        self._outputs = None
        self._checksum_value = None
        self._app_name = spec.app_name

    @property
    def outputs(self) -> TargetOutputs:
        """由于在计算输出路径时，会涉及到依赖，所以不能在 __init__ 中计算"""
        if self._outputs is None:
            # 处理继承依赖的输出的情况
            if self._output_spec.get("inherit", False):
                paths = self._output_spec.get("paths", [])
                for dep in self.dependencies:
                    if isinstance(dep, DependencyTypeTarget):
                        paths.extend(dep.target.outputs.paths)
                self._output_spec["paths"] = paths
            self._outputs = TargetOutputs(self._output_spec)
        return self._outputs

    def validate(self) -> None:
        for dep in self.dependencies:
            dep.validate()
        self.outputs.validate()
        self.operations.validate()

        if not re.match(r"^[a-z0-9\-:]+$", self.name):
            raise SpecError(
                f"Target {self.name} 的名称应是由小写字母、数字、英文冒号和连字符（-）组成"
            )

        if len(self.name) > 48:
            raise SpecError(f"Target {self.name} 的名称过长")

        if len(self.description) > 255:
            raise SpecError(f"Target {self.name} 的描述过长")

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

    @staticmethod
    def get_by_name(name: str) -> Target:
        from quack.spec import Spec

        try:
            return Spec.get().targets[name]
        except KeyError:
            logger.critical(f"未找到 Target {name}")
            sys.exit(1)

    @staticmethod
    def get_by_job(
        db: DB,
        ci_environment: CIEnvironment,
        app_name: str,
        job_name: str,
        target_name: str,
    ) -> Target:
        """从数据库加载并执行指定job的target"""
        target_checksum = db.query_checksum(
            app_name, ci_environment.commit_sha, job_name, target_name
        )
        if target_checksum is None:
            raise DBNotFoundError(
                f"数据库中未找到对应的 Checksum，Job Name: {job_name}, Target Name: {target_name}, Commit SHA: {ci_environment.commit_sha}"
            )
        target = Target.get_by_name(target_name)
        target.checksum_value = target_checksum.checksum
        return target

    def compute_checksum(self) -> str:
        hash_tuple = [dep.checksum_value for dep in self.dependencies]
        logger.debug(f"Target {self.name} 各依赖 Checksum 值：")
        for dep in self.dependencies:
            logger.debug(f"- {dep.display_name}: {dep.checksum_value}")
        return hashlib.sha256(repr(hash_tuple).encode("utf-8")).hexdigest()

    def prepare_deps(
        self, config: Config, cache_backend: type[TargetCacheBackendType]
    ) -> None:
        for dep in self.dependencies:
            if isinstance(dep, DependencyTypeTarget):
                dep.target.execute(config, cache_backend)

    def execute(
        self,
        config: Config,
        cache_backend: type[TargetCacheBackendType],
        mode: TargetExecutionMode = TargetExecutionMode.NORMAL,
    ) -> None:
        logger.info(f"正在执行 Target {self.name}...")

        from quack.cache import TargetCache, TargetCacheBackendTypeServe

        logger.info(f"Target {self.name} Checksum 值：{self.checksum_value}")
        logger.info(f"正在查找 Target {self.name} 的缓存...")

        cache = TargetCache(config, self._app_name, self, cache_backend)
        cache_exists = cache.hit()

        if mode == TargetExecutionMode.DEPS_ONLY:
            self.prepare_deps(config, cache_backend)
        elif mode == TargetExecutionMode.LOAD_ONLY:
            if cache_exists:
                logger.info("找到缓存，直接从缓存加载...")
                cache.load()
            else:
                logger.error("未找到缓存，无法进行加载")
                sys.exit(1)
        else:
            # Serve 模式下，即使缓存存在，也生成依赖项，以便回传给客户端
            if cache_backend == TargetCacheBackendTypeServe:
                self.prepare_deps(config, cache_backend)

            if not cache_exists:
                logger.info(
                    f"未找到对应的缓存，开始重新生成缓存：{self.operations.build.command}"
                )
                # 跳过依赖的执行
                if cache_backend != TargetCacheBackendTypeServe:
                    self.prepare_deps(config, cache_backend)
                self.operations.build.execute()

            if cache_exists:
                logger.info("找到缓存，直接从缓存加载...")
                cache.load()
            else:
                logger.info(f"正在存入缓存，路径：{self.cache_path}")
                try:
                    cache.save()
                except OSSError as e:
                    logger.error(f"{e}\n{e.stdout}\n{e.stderr}")
                    sys.exit(1)

        logger.success("执行完毕！")
