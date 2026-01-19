#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from subprocess import CalledProcessError

from loguru import logger

from quack.cache import TargetCacheBackendType, TargetCacheBackendTypeCloud
from quack.config import Config
from quack.models.target import TargetExecutionMode
from quack.spec import Spec
from quack.utils.ci_environment import CIEnvironment


def execute_script(spec: Spec, name: str, arguments: list[str]) -> None:
    try:
        script = spec.scripts[name]
    except KeyError:
        logger.critical(f"未找到脚本 {name}")
        sys.exit(1)

    try:
        script.execute(arguments)
    except CalledProcessError:
        logger.error(f"脚本 {name} 执行失败")
        sys.exit(1)


def execute_target(
    spec: Spec,
    app_name: str,
    name: str,
    cache_backend: type[TargetCacheBackendType],
    mode: TargetExecutionMode,
    config: Config,
) -> None:
    try:
        target = spec.targets[name]
    except KeyError:
        logger.critical(f"未找到 Target {name}")
        sys.exit(1)

    cloud_backend = TargetCacheBackendTypeCloud(config, app_name)
    commit_metadata_path = cloud_backend.get_commit_metadata_path(target)
    if mode == TargetExecutionMode.LOAD_ONLY:
        metadata = cloud_backend.cloud_client.read(commit_metadata_path)
        if metadata is None:
            logger.error(f"加载失败，未找到 Target {name} 的缓存")
            sys.exit(1)
        target.checksum_value = json.loads(metadata)["target_checksum"]

    try:
        target.execute(config, app_name, cache_backend, mode)
    except CalledProcessError:
        logger.error(f"Target {name} 执行失败")
        sys.exit(1)

    # 记录成功执行的 target metadata，方便根据 commit sha 进行 load
    if config.save_for_load and CIEnvironment.is_ci:
        cloud_backend.cloud_client.upload(
            cloud_backend.local_backend.get_metadata_path(target),
            cloud_backend.get_commit_metadata_path(target),
        )
