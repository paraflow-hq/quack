#!/usr/bin/env python3

import os
import signal
import subprocess
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import Field, computed_field, field_validator, model_validator

from quack.models.base import BaseModel


class Command(BaseModel):
    command: str
    base_path: Path = Field(default_factory=Path)
    path: Path = Field(default_factory=Path)
    variables: dict[str, str] = Field(default_factory=dict)

    _process: subprocess.Popen | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: dict[str, Any]) -> dict[str, Any]:
        if isinstance(data, str):
            return {"command": data}
        else:
            return data

    @field_validator("variables")
    @classmethod
    def validate_variables(cls, v: dict[str, str]) -> dict[str, str]:
        environ = dict(os.environ)
        environ.update(v)
        return environ

    @computed_field
    @property
    def cwd(self) -> Path:
        return self.base_path.joinpath(self.path).resolve()

    def execute(self, args: list[str] | None = None) -> None:
        from quack.services.command_manager import CommandManager

        command = " && ".join(self.command.strip().split("\n"))
        if args:
            command = " ".join((command, *args))

        logger.info(f"正在执行命令 `{command}`...")
        try:
            CommandManager.get().register(self)
            self._process = subprocess.Popen(
                command,
                cwd=self.cwd,
                env=self.variables,
                shell=True,
                start_new_session=True,
            )
            returncode = self._process.wait()
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, command)
        finally:
            CommandManager.get().unregister(self)

    def terminate(self) -> None:
        """终止命令执行"""
        if self._process:
            try:
                # 终止整个进程组
                os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass  # 进程可能已经结束
