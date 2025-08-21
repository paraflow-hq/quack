#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from dataclasses import dataclass

from loguru import logger

from quack.exceptions import SpecError
from quack.models.command import Command


@dataclass
class Script:
    name: str
    description: str
    command: Command

    def __init__(self, data) -> None:
        from quack.spec import Spec

        self.name = data["name"]
        self.description = data["description"]
        self.command = Command(data["command"], Spec.get().pwd)

    @staticmethod
    def get_by_name(name: str) -> Script:
        from quack.spec import Spec

        try:
            return Spec.get().scripts[name]
        except KeyError:
            logger.critical(f"未找到脚本 {name}")
            sys.exit(1)

    def validate(self) -> None:
        if not re.match(r"^[a-z0-9\-_\.]+$", self.name):
            raise SpecError(
                f"脚本 {self.name} 的名称应是由小写字母、数字、小数点（.）、下划线（_）和连字符（-）组成"
            )

        if len(self.name) > 32:
            raise SpecError(f"脚本 {self.name} 的名称过长")

        if len(self.description) > 255:
            raise SpecError(f"脚本 {self.name} 的描述过长")

        self.command.validate()

    def execute(self, args: list[str] | None = None) -> None:
        self.command.execute(args)

    def terminate(self) -> None:
        self.command.terminate()
