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
    TargetCacheBackendTypeCloud,
    TargetCacheBackendTypeLocal,
    TargetCacheBackendTypeMap,
)
from quack.cli import execute_script, execute_target
from quack.config import Config, LogLevel
from quack.models.target import TargetExecutionMode
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


def exit_handler(config: Config, app_name: str) -> None:
    CommandManager.get().terminate_all()
    # 退出时定期清理本地缓存
    TargetCacheBackendTypeLocal(config, app_name).clear_expired()


@dataclass
class QuackArgs(argparse.Namespace):
    list_all: bool
    directory: str
    load_only: bool
    clear_expired_cache: bool
    deps_only: bool
    names: list[str]
    cache: str
    log_level: str
    list: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quack - 带缓存的构建执行工具")
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
        "--save-for-commit",
        action="store_true",
        help="将产物保存到缓存中，以便 CI 按 commit sha 加载",
    )
    _ = parser.add_argument(
        "--load-only",
        action="store_true",
        help="不执行 Target，而是从 CI 中已经编译的缓存中按 commit sha 加载 target",
    )
    _ = parser.add_argument(
        "--clear-expired-cache",
        action="store_true",
        help="清理过期的缓存",
    )
    _ = parser.add_argument(
        "--deps-only",
        action="store_true",
        help="仅准备依赖，不执行 Target 本身",
    )
    _ = parser.add_argument(
        "names",
        nargs="*",
        help="要执行的一个或多个脚本或 Target 名称",
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

    scripts = {name: script for name, script in spec.scripts.items() if not name.startswith(".")}
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


def init_spec(spec_path: Path, cwd: Path) -> Spec:
    spec = Spec.from_file(spec_path, cwd)
    return spec


def get_spec_path(cwd: Path) -> Path:
    while cwd != cwd.parent:
        spec_path = cwd / "quack.yaml"
        # FIXME: 使用更合理的方式判断 root path
        if spec_path.exists() and spec_path.read_text().startswith("app_name: "):
            return spec_path
        cwd = cwd.parent
    else:
        logger.error("未找到 quack.yaml 配置文件")
        sys.exit(1)


def main():
    args = cast(QuackArgs, parse_args())

    cwd = Path(args.directory).expanduser().resolve() if args.directory else Path(os.getcwd())

    # 找到并切换到根目录
    spec_path = get_spec_path(cwd)
    os.chdir(spec_path.parent)

    # 读取配置
    config = Config()
    config.cache = args.cache or config.cache
    config.setup_runtime()

    log_level = args.log_level or config.log_level.value
    _ = logger.remove()
    _ = logger.add(sys.stderr, level=log_level)

    ci_environment = CIEnvironment()

    spec = init_spec(spec_path, cwd)

    # 注册信号和退出处理器
    _ = signal.signal(signal.SIGINT, _signal_handler)  # pyright: ignore[reportUnknownArgumentType]
    _ = signal.signal(signal.SIGTERM, _signal_handler)  # pyright: ignore[reportUnknownArgumentType]
    _ = atexit.register(exit_handler, config, spec.app_name)

    if args.clear_expired_cache:
        TargetCacheBackendTypeCloud(config, spec.app_name).clear_expired()
        sys.exit(0)

    if args.list or args.list_all:
        print_available_items(spec, args.list_all)
        sys.exit(0)

    if len(args.names) == 0:
        logger.error("请指定要执行的脚本或 Target")
        sys.exit(1)

    if args.load_only:
        if len(args.names) != 1:
            logger.error("load-only 模式下只能指定一个 Target 名称")
            sys.exit(1)
        if not ci_environment.is_ci:
            logger.error("load-only 模式仅支持在 CI 环境执行")
            sys.exit(1)

    name: str = args.names[0]
    arguments: list[str] = args.names[1:]
    if name in spec.scripts:
        execute_script(spec, name, arguments)
    elif name in spec.targets:
        if args.load_only:
            mode = TargetExecutionMode.LOAD_ONLY
        elif args.deps_only:
            mode = TargetExecutionMode.DEPS_ONLY
        else:
            mode = TargetExecutionMode.NORMAL

        execute_target(
            spec,
            spec.app_name,
            name,
            TargetCacheBackendTypeMap[config.cache],
            mode,
            config,
        )
    else:
        logger.error(f"无效的脚本或者 Target 名称：{name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
