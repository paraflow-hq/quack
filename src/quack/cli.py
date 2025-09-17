#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from subprocess import CalledProcessError

from loguru import logger

from quack.cache import TargetCacheBackendType, TargetCacheBackendTypeOSS
from quack.config import Config
from quack.models.script import Script
from quack.models.target import Target, TargetExecutionMode
from quack.services.command_manager import CommandManager
from quack.spec import Spec
from quack.utils.ci_environment import CIEnvironment


def execute_scripts_parallel(names: list[str], spec: Spec) -> None:
    """并行执行多个脚本，任一脚本执行失败则终止所有脚本"""
    if len(names) == 1:
        logger.error("并行模式下至少需要指定两个脚本或 Target")
        sys.exit(1)

    # 检查脚本和目标名称
    script_names = [name for name in names if name in spec.scripts]
    target_names = [name for name in names if name in spec.targets]
    other_names = [
        name for name in names if name not in spec.scripts and name not in spec.targets
    ]

    if other_names:
        logger.error(f"无效的脚本或者 Target 名称：{', '.join(other_names)}")
        sys.exit(1)

    if target_names:
        logger.error("并行模式下只能执行脚本，不能执行 Target")
        sys.exit(1)

    with ThreadPoolExecutor() as executor:
        futures: list[tuple[Script, Future[None]]] = []
        for script_name in script_names:
            script = Script.get_by_name(script_name)
            future = executor.submit(script.execute)
            futures.append((script, future))

        # 使用 as_completed 等待任务完成，这样可以立即发现失败的任务
        name_future_map = {future: script.name for script, future in futures}
        for future in as_completed([f for _, f in futures]):
            name = name_future_map[future]
            try:
                future.result()
                logger.success(f"脚本 {name} 执行成功")
            except Exception:
                logger.error(f"脚本 {name} 执行失败")
                CommandManager.get().terminate_all()
                sys.exit(1)


def execute_script(name: str, arguments: list[str]) -> None:
    script = Script.get_by_name(name)
    try:
        script.execute(arguments)
    except CalledProcessError:
        logger.error(f"脚本 {name} 执行失败")
        sys.exit(1)


def execute_target(
    app_name: str,
    name: str,
    cache_backend: type[TargetCacheBackendType],
    mode: TargetExecutionMode,
    config: Config,
) -> None:
    target = Target.get_by_name(name)
    oss_backend = TargetCacheBackendTypeOSS(config, app_name)
    commit_metadata_path = oss_backend.get_commit_metadata_path(target)
    if mode == TargetExecutionMode.LOAD_ONLY:
        metadata = oss_backend.oss_client.read(commit_metadata_path)
        if metadata is None:
            logger.error(f"加载失败，未找到 Target {name} 的缓存")
            sys.exit(1)
        target.checksum_value = json.loads(metadata)["target_checksum"]

    try:
        target.execute(config, cache_backend, mode)
    except CalledProcessError:
        logger.error(f"Target {name} 执行失败")
        sys.exit(1)

    # 记录成功执行的 target metadata，方便根据 commit sha 进行 load
    if config.save_for_load and CIEnvironment.is_ci:
        oss_backend.oss_client.upload(
            oss_backend.local_backend.get_metadata_path(target),
            oss_backend.get_commit_metadata_path(target),
        )
