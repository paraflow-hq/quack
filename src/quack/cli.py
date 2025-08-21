#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from subprocess import CalledProcessError

from loguru import logger

from quack.cache import TargetCacheBackendType
from quack.config import Config
from quack.consts import SERVE_BASE_PATH
from quack.db import DB, TargetChecksum
from quack.exceptions import DBNotFoundError
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
    db: DB | None,
    ci_environment: CIEnvironment,
    job_name: str,
) -> None:
    if mode == TargetExecutionMode.LOAD_ONLY:
        assert db
        try:
            target = Target.get_by_job(db, ci_environment, app_name, job_name, name)
        except DBNotFoundError as e:
            logger.error(e)
            sys.exit(1)
    else:
        target = Target.get_by_name(name)

    try:
        target.execute(config, cache_backend, mode)
    except CalledProcessError:
        logger.error(f"Target {name} 执行失败")
        sys.exit(1)

    # Merge Train 执行完毕后，将构建的 checksum 存入数据库，以供其他流水线共享缓存
    if db and ci_environment.is_ci and ci_environment.is_merge_group:
        db.record_checksum(
            TargetChecksum(
                app_name=app_name,
                commit_sha=ci_environment.commit_sha,
                mr_iid=ci_environment.pr_id,
                pipeline_id=ci_environment.pipeline_id,
                job_name=ci_environment.job_name,
                target_name=target.name,
                checksum=target.checksum_value,
            )
        )


def execute_remote(name: str, mode: TargetExecutionMode, config: Config):
    cmd = ["quack-remote", name]
    if mode == TargetExecutionMode.DEPS_ONLY:
        cmd.append("--deps-only")

    env = dict(os.environ)
    env["REMOTE_HOST"] = config.remote_host
    env["REMOTE_ROOT"] = config.remote_root
    env["SERVE_BASE_PATH"] = SERVE_BASE_PATH
    try:
        _ = subprocess.run(cmd, env=env, check=True)
    except CalledProcessError:
        logger.error(f"Target {name} 执行失败")
        sys.exit(1)
