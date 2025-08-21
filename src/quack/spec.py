#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import yaml
from loguru import logger

from quack.exceptions import SpecError
from quack.models.script import Script
from quack.models.target import Target


@dataclass
class Spec:
    _INSTANCE: ClassVar[Spec | None] = None

    app_name: str
    path: Path
    pwd: Path
    global_dependencies: dict[str, dict[str, Any]]
    targets: dict[str, Target]
    scripts: dict[str, Script]

    def __init__(
        self, pwd: Path, path: Path, data: dict[str, Any] | None = None
    ) -> None:
        if Spec._INSTANCE is None:
            Spec._INSTANCE = self

        self.app_name = ""
        self.pwd = pwd
        self.path = path.resolve()
        self.global_dependencies = {}
        self.targets = {}
        self.scripts = {}

        if data is None:
            data = self.read(self.path)
        self.parse(data)

        # 递归解析所有配置文件
        for include_path in data.get("include", []):
            self.merge(Spec(pwd, Path(include_path) / "quack.yaml"))

    def read(self, path: Path) -> dict[str, Any]:
        logger.debug(f"正在解析配置文件 {path}...")

        if not path.exists():
            logger.critical(f"配置文件 {path} 不存在")
            sys.exit(1)

        return yaml.safe_load(path.read_text())

    def parse(self, data: dict[str, Any]):
        # 仅在根目录下加载这些全局配置
        if self.path.parent == Path(os.getcwd()):
            self.app_name = data["app_name"]
            for dep in data.get("global_dependencies", []):
                self.add_global_dependency(dep)

        for target_config in data.get("targets", []):
            target = Target(target_config)
            self.add_target(target)

        # 仅加载 quack 执行目录下的 scripts 配置
        if self.path.parent == self.pwd:
            for script_config in data.get("scripts", {}):
                script = Script(script_config)
                self.add_script(script)

    def add_global_dependency(self, data: dict[str, Any]) -> None:
        if data["name"] in self.global_dependencies:
            raise SpecError(f"全局依赖 {data['name']} 名称重复")
        else:
            self.global_dependencies[data["name"]] = data

    def add_target(self, target: Target) -> None:
        if target.name in self.targets or target.name in self.scripts:
            raise SpecError(f"Target {target.name} 名称重复")
        else:
            self.targets[target.name] = target

    def add_script(self, script: Script) -> None:
        if script.name in self.targets or script.name in self.scripts:
            raise SpecError(f"Script {script.name} 名称重复")
        else:
            self.scripts[script.name] = script

    def merge(self, config: Spec) -> None:
        for target in config.targets.values():
            self.add_target(target)

        for script in config.scripts.values():
            self.add_script(script)

    def validate(self) -> None:
        if not self.app_name:
            raise SpecError("app_name 未设置")

        for target in self.targets.values():
            target.validate()

        for script in self.scripts.values():
            script.validate()

    @classmethod
    def get(cls) -> Spec:
        assert cls._INSTANCE is not None
        return cls._INSTANCE
