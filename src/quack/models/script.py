#!/usr/bin/env python3

from __future__ import annotations

import time
from pathlib import Path

from loguru import logger
from pydantic import Field, ValidationInfo, computed_field, field_validator

from quack.models.base import BaseModel
from quack.models.command import Command
from quack.utils.formatter import format_duration


class Script(BaseModel):
    name: str = Field(max_length=32, pattern=r"^[a-z0-9\-_\.]+$")
    description: str = Field(max_length=255)
    base_path: Path = Field(default_factory=Path)
    module_path: Path = Field(default_factory=Path)  # quack.yaml 文件所在目录
    command: Command

    @computed_field
    @property
    def display_name(self) -> str:
        """获取用于显示的完整名称,格式: <module_name>/<script_name>"""
        module_name = self.module_path.name
        return f"{module_name}/{self.name}" if module_name else self.name

    @field_validator("command")
    @classmethod
    def validate_command(cls, v: Command, info: ValidationInfo) -> Command:
        """脚本命令的默认执行目录为终端的当前工作目录"""
        v.base_path = info.data["base_path"]
        return v

    def execute(self, args: list[str] | None = None) -> None:
        start_time = time.time()
        self.command.execute(args)
        elapsed = time.time() - start_time
        logger.info(f"脚本 {self.display_name} 执行耗时: {format_duration(elapsed)}")

    def terminate(self) -> None:
        self.command.terminate()
