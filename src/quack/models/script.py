#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from pydantic import Field, ValidationInfo, field_validator

from quack.models.base import BaseModel
from quack.models.command import Command


class Script(BaseModel):
    name: str = Field(max_length=32, pattern=r"^[a-z0-9\-_\.]+$")
    description: str = Field(max_length=255)
    base_path: Path = Field(default_factory=Path)
    command: Command

    @field_validator("command")
    @classmethod
    def validate_command(cls, v: Command, info: ValidationInfo) -> Command:
        """脚本命令的默认执行目录为终端的当前工作目录"""
        v.base_path = info.data["base_path"]
        return v

    def execute(self, args: list[str] | None = None) -> None:
        self.command.execute(args)

    def terminate(self) -> None:
        self.command.terminate()
