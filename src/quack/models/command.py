#!/usr/bin/env python3

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger


@dataclass
class Command:
    command: str
    path: Path
    variables: dict[str, str]

    def __init__(
        self, data: str | dict[str, Any], base_path: Path | None = None
    ) -> None:
        if base_path is None:
            base_path = Path(os.getcwd())

        if isinstance(data, str):
            self.command = data
            self.path = base_path
            self.variables = dict(os.environ)
        else:
            self.command = data["command"]
            self.path = base_path / data.get("path", "")
            self.variables = dict(os.environ)
            self.variables.update(data.get("variables", {}))

        # 用于跟踪子进程
        self.process = None

    def validate(self) -> None:
        pass

    def execute(self, args: list[str] | None = None) -> None:
        from quack.services.command_manager import CommandManager

        command = " && ".join(self.command.strip().split("\n"))
        if args:
            command = " ".join((command, *args))

        logger.info(f"正在执行命令 `{command}`...")
        try:
            CommandManager.get().register(self)
            self.process = subprocess.Popen(
                command,
                cwd=self.path,
                env=self.variables,
                shell=True,
                start_new_session=True,
            )
            returncode = self.process.wait()
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, command)
        finally:
            CommandManager.get().unregister(self)

    def terminate(self) -> None:
        """终止命令执行"""
        if self.process:
            try:
                # 终止整个进程组
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass  # 进程可能已经结束
