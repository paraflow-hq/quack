#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from functools import cache
from pathlib import Path
from typing import Any, ClassVar

import yaml
from loguru import logger
from pydantic import (
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
)

from quack.models.base import BaseModel
from quack.models.dependency import Dependency
from quack.models.script import Script
from quack.models.target import Target


class Spec(BaseModel):
    _INSTANCE: ClassVar[Spec | None] = None

    # TODO: 添加 app_name 存在的校验
    app_name: str = Field(default="", max_length=32, pattern=r"^[a-z0-9\-_]+$", validate_default=False)
    path: Path = Field(default_factory=Path)
    cwd: Path = Field(default_factory=Path)
    global_dependencies: dict[str, Dependency] = Field(default_factory=dict)
    targets: dict[str, Target] = Field(default_factory=dict)
    scripts: dict[str, Script] = Field(default_factory=dict)
    include: list[Spec] = Field(default_factory=list)

    def __init__(self, *args, **kwargs) -> None:
        if Spec._INSTANCE is None:
            Spec._INSTANCE = self

        super().__init__(*args, **kwargs)

    @field_validator("global_dependencies", mode="before")
    def validate_global_dependencies(cls, v: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        names = set()
        for dep in v:
            if dep["name"] in names:
                raise ValueError(f"全局依赖 {dep['name']} 名称重复")
            names.add(dep["name"])
        return {dep["name"]: dep for dep in v}

    @field_validator("targets", mode="before")
    def validate_targets(cls, v: list[dict[str, Any]], info: ValidationInfo) -> dict[str, dict[str, Any]]:
        # 先处理全局依赖
        names = set()
        module_path = Path(info.data["path"]).parent
        for target in v:
            if target["name"] in names:
                raise ValueError(f"目标 {target['name']} 名称重复")
            names.add(target["name"])

            target["module_path"] = str(module_path)

        return {target["name"]: target for target in v}

    @field_validator("scripts", mode="before")
    def validate_scripts(cls, v: list[dict[str, Any]], info: ValidationInfo) -> dict[str, dict[str, Any]]:
        # 仅加载 quack 执行目录下的 scripts 配置
        if info.data["path"].parent == info.data["cwd"]:
            names = set()
            module_path = Path(info.data["path"]).parent
            for script in v:
                if script["name"] in names:
                    raise ValueError(f"脚本 {script['name']} 名称重复")
                names.add(script["name"])

                script["base_path"] = info.data["cwd"]
                script["module_path"] = str(module_path)
            return {script["name"]: script for script in v}
        else:
            return {}

    @field_validator("include", mode="before")
    def validate_include(cls, v: list[str], info: ValidationInfo) -> list[Spec]:
        specs = []
        for path in v:
            spec_path = Path(path) / "quack.yaml"
            spec = Spec.from_file(spec_path, info.data["cwd"])
            specs.append(spec)
        return specs

    @classmethod
    def from_file(cls, path: Path, cwd: Path) -> Spec:
        logger.debug(f"正在解析配置文件 {path}...")

        if not path.exists():
            logger.critical(f"配置文件 {path} 不存在")
            sys.exit(1)

        data = yaml.safe_load(path.read_text())
        data["cwd"] = str(cwd.resolve())
        data["path"] = str(path.resolve())

        try:
            spec = Spec.model_validate_json(json.dumps(data))
        except ValidationError as e:
            logger.error(f"配置文件错误: {e}")
            sys.exit(1)

        # 对于顶层 spec 做额外处理
        if spec.app_name:
            spec.post_process()

        return spec

    def add_target(self, target: Target) -> None:
        if target.name in self.targets or target.name in self.scripts:
            raise ValueError(f"Target {target.name} 名称重复")
        else:
            self.targets[target.name] = target

    def add_script(self, script: Script) -> None:
        if script.name in self.targets or script.name in self.scripts:
            raise ValueError(f"Script {script.name} 名称重复")
        else:
            self.scripts[script.name] = script

    def post_process(self) -> None:
        for spec in self.include:
            for target in spec.targets.values():
                self.add_target(target)

            for script in spec.scripts.values():
                self.add_script(script)

        global_deps_to_propagate = [dep for dep in self.global_dependencies.values() if dep.propagate]
        for target in self.targets.values():
            target.dependencies[:0] = global_deps_to_propagate
            for dep in target.dependencies:
                if dep.type == "global":
                    if dep.name not in self.global_dependencies:
                        raise ValueError(f"全局依赖 {dep.name} 不存在")
                    target.dependencies[target.dependencies.index(dep)] = self.global_dependencies[dep.name]

        targets = self.targets

        # 后处理输出继承
        @cache
        def get_outputs(target_name: str) -> set[str]:
            """递归获取输出路径"""
            target = targets[target_name]
            outputs = target.outputs.paths
            if target.outputs.inherit:
                for dep in target.dependencies:
                    if dep.type == "target":
                        outputs.update(get_outputs(dep.name))
            return outputs

        for target in targets.values():
            if target.outputs.inherit:
                target.outputs.paths = get_outputs(target.name)

    @classmethod
    def get(cls) -> Spec:
        assert cls._INSTANCE is not None
        return cls._INSTANCE
