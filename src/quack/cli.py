#!/usr/bin/env python3

from __future__ import annotations

import sys
from subprocess import CalledProcessError

from loguru import logger

from quack.cache import TargetCacheBackendType, TargetCacheBackendTypeCloud
from quack.config import CacheAccess, Config
from quack.exceptions import CacheSecurityError, ConfigError
from quack.models.target import TargetExecutionMode
from quack.spec import Spec
from quack.utils.ci_environment import CIEnvironment
from quack.utils.metadata import Metadata


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

    ci = CIEnvironment()
    remote_enabled = config.cache in ("cloud", "dev") and config.cache_access != CacheAccess.NONE
    if mode == TargetExecutionMode.LOAD_ONLY:
        if not remote_enabled:
            raise ConfigError("load-only requires an enabled remote cache")
        cloud_backend = TargetCacheBackendTypeCloud(config, app_name)
        commit_metadata_path = cloud_backend.get_commit_metadata_path(target, ci.commit_sha)
        metadata = cloud_backend.cloud_client.read(commit_metadata_path)
        if metadata is None:
            logger.error(f"加载失败，未找到 Target {name} 的缓存")
            sys.exit(1)
        index = Metadata.parse(metadata)
        if index.target_name != target.name or index.commit_sha != ci.commit_sha:
            raise CacheSecurityError("Cache index does not match the requested target and commit")
        if index.format_version == 3:
            cloud_backend.restore_commit(target, index)
            return
        target.checksum_value = index.target_checksum
        target.expected_archive_checksum = index.file_checksum

    try:
        target.execute(config, app_name, cache_backend, mode)
    except CalledProcessError:
        logger.error(f"Target {name} 执行失败")
        sys.exit(1)

    # 记录成功执行的 target metadata，方便根据 commit sha 进行 load
    if (
        config.save_for_load
        and ci.is_ci
        and remote_enabled
        and config.cache_access == CacheAccess.READ_WRITE
        and mode == TargetExecutionMode.NORMAL
    ):
        TargetCacheBackendTypeCloud(config, app_name).publish_commit(target, ci.commit_sha)
