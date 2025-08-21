#!/usr/bin/env python3

import argparse
import atexit
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from loguru import logger

from quack.cache import (
    TargetCacheBackendTypeLocal,
    TargetCacheBackendTypeMap,
    TargetCacheBackendTypeOSS,
)
from quack.cli import (
    execute_remote,
    execute_script,
    execute_scripts_parallel,
    execute_target,
)
from quack.config import Config, DBConfig, LogLevel
from quack.db import DB
from quack.exceptions import SpecError
from quack.models.target import TargetExecutionMode
from quack.runtime import RuntimeState
from quack.services.command_manager import CommandManager
from quack.spec import Spec
from quack.utils.ci_environment import CIEnvironment

SCRIPT_DIR = Path(__file__).parent.resolve()


def _signal_handler(signum: int, _) -> None:
    """统一的信号处理函数"""
    if signum == signal.SIGINT:
        logger.info("用户主动中断执行")
    CommandManager.get().terminate_all()
    sys.exit(1)


def exit_handler(db: DB | None, config: Config, app_name: str) -> None:
    CommandManager.get().terminate_all()
    # 退出时定期清理本地缓存
    TargetCacheBackendTypeLocal(config, app_name).clear_expired()
    if db is not None:
        db.commit()
        db.close()


@dataclass
class QuackArgs(argparse.Namespace):
    list_all: bool
    directory: str
    load_from_job: str
    clear_expired_cache: bool
    remote: bool
    deps_only: bool
    test_features: str
    names: list[str]
    parallel: bool
    cache: str
    log_level: str
    list: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quack - 带缓存的构建执行工具",
        epilog="📖 使用手册: https://www.notion.so/kanyun/Quack-15866f1452e280b89209e0ef93ae415e?pvs=4",
    )
    _ = parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="列出所有可用的脚本",
    )
    _ = parser.add_argument(
        "--list-all",
        "-L",
        action="store_true",
        help="列出所有可用的脚本和 Target",
    )
    _ = parser.add_argument(
        "--directory",
        "-C",
        help="设置工作目录，执行前会先切换到该目录",
    )
    _ = parser.add_argument(
        "--load-from-job",
        help="从指定的 job 中加载 target checksum",
    )
    _ = parser.add_argument(
        "--clear-expired-cache",
        action="store_true",
        help="清理过期的缓存",
    )
    _ = parser.add_argument(
        "--remote",
        action="store_true",
        help="远程执行 Target",
    )
    _ = parser.add_argument(
        "--deps-only",
        action="store_true",
        help="仅准备依赖，不执行 Target 本身",
    )
    _ = parser.add_argument(
        "--test-features",
        choices=["default", "on"],
        help="如果设置为 on，执行测试时会开启所有开关",
    )
    _ = parser.add_argument(
        "names",
        nargs="*",
        help="要执行的一个或多个脚本或 Target 名称",
    )
    _ = parser.add_argument(
        "--parallel",
        "-p",
        action="store_true",
        help="并行执行多个脚本",
    )
    _ = parser.add_argument(
        "--cache",
        choices=["false", "local", "dev"],
        help="指定缓存后端类型，false 表示禁用缓存",
    )
    _ = parser.add_argument(
        "--log-level",
        choices=LogLevel,
        help="设置日志等级",
    )

    return parser.parse_args()


def print_available_items(spec: Spec, list_targets: bool) -> None:
    """打印所有可用的脚本和目标"""

    print()

    scripts = {
        name: script
        for name, script in spec.scripts.items()
        if not name.startswith(".")
    }
    if scripts:
        print("📜 脚本（仅当前目录可用）\n")
        for name, script in sorted(scripts.items()):
            if not name.startswith("."):
                print(f"  *  {name:32} - {script.description}")

    if list_targets and spec.targets:
        if scripts:
            print()
        print("🎯 Targets（全局可用，主要用于 CI）\n")
        for name, target in sorted(spec.targets.items()):
            print(f"  *  {name:32} - {target.description}")

    print()
    print(
        "📖 Quack 使用手册: https://www.notion.so/kanyun/Quack-15866f1452e280b89209e0ef93ae415e?pvs=4\n"
    )


def init_spec(pwd: Path, spec_path: Path, is_nested: bool) -> Spec:
    spec = Spec(pwd, spec_path)
    if not is_nested:
        try:
            spec.validate()
        except SpecError as e:
            logger.error(f"配置文件错误: {e}")
            sys.exit(1)
    return spec


def init_db(db_config: DBConfig) -> DB | None:
    return DB(
        db_config.host,
        db_config.port,
        db_config.user,
        db_config.password,
        db_config.database,
    )


def get_spec_path(pwd: Path) -> Path:
    while pwd != pwd.parent:
        spec_path = pwd / "quack.yaml"
        if spec_path.exists() and spec_path.read_text().startswith("app_name: "):
            return spec_path
        pwd = pwd.parent
    else:
        logger.error("未找到 quack.yaml 配置文件")
        sys.exit(1)


def main():
    args = cast(QuackArgs, parse_args())

    if args.directory:
        pwd = Path(args.directory).expanduser().resolve()
    else:
        pwd = Path(os.getcwd())

    # 找到并切换到根目录
    spec_path = get_spec_path(pwd)
    os.chdir(spec_path.parent)

    # 读取配置
    config = Config()

    log_level = args.log_level or config.log_level.value
    _ = logger.remove()
    _ = logger.add(sys.stderr, level=log_level)

    # Runtime 需要最先初始化，以便子进程能够正确读取环境变量
    runtime = RuntimeState(
        pwd,
        dict(os.environ),
        args.cache or config.cache,
        args.test_features,
    )
    runtime.setup()

    ci_environment = CIEnvironment()
    db = init_db(config.db) if ci_environment.is_ci else None

    spec = init_spec(pwd, spec_path, runtime.is_nested)

    # 注册信号和退出处理器
    _ = signal.signal(signal.SIGINT, _signal_handler)  # pyright: ignore[reportUnknownArgumentType]
    _ = signal.signal(signal.SIGTERM, _signal_handler)  # pyright: ignore[reportUnknownArgumentType]
    _ = atexit.register(exit_handler, db, config, spec.app_name)

    if args.clear_expired_cache:
        TargetCacheBackendTypeOSS(config, spec.app_name).clear_expired()
        sys.exit(0)

    if args.list or args.list_all:
        print_available_items(spec, args.list_all)
        sys.exit(0)

    if len(args.names) == 0:
        logger.error("请指定要执行的脚本或 Target")
        sys.exit(1)

    if args.parallel:
        execute_scripts_parallel(args.names, spec)
        sys.exit(0)

    if args.load_from_job:
        if len(args.names) != 1:
            logger.error("load-from-job 模式下只能指定一个 Target 名称")
            sys.exit(1)
        if not ci_environment.is_ci:
            logger.error("load-from-job 模式仅支持在 CI 环境执行")
            sys.exit(1)

    if args.remote:
        if not config.remote_host:
            logger.error(
                "远程执行模式下需要在配置文件中指定远程主机名，详见 notion 文档"
            )
            sys.exit(1)

    name: str = args.names[0]
    arguments: list[str] = args.names[1:]
    if name in spec.scripts:
        execute_script(name, arguments)
    elif name in spec.targets:
        if args.load_from_job:
            mode = TargetExecutionMode.LOAD_ONLY
        elif args.deps_only:
            mode = TargetExecutionMode.DEPS_ONLY
        else:
            mode = TargetExecutionMode.NORMAL

        if args.remote:
            execute_remote(name, mode, config)
        else:
            execute_target(
                spec.app_name,
                name,
                TargetCacheBackendTypeMap[runtime.cache],
                mode,
                config,
                db,
                ci_environment,
                job_name=args.load_from_job,
            )
    else:
        logger.error(f"无效的脚本或者 Target 名称：{name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
