#!/usr/bin/env python3

import hashlib
import os
import re
import subprocess
import sys
from collections import defaultdict
from functools import cached_property
from itertools import chain
from typing import Annotated, Literal

from loguru import logger
from pydantic import Field, field_validator

from quack.models.base import BaseModel
from quack.models.command import Command
from quack.utils.checksummer import generate_sha256sum
from quack.utils.ci_environment import CIEnvironment


class DependencyTypeSource(BaseModel):
    type: Literal["source"]
    paths: list[str]
    excludes: list[str] = Field(default_factory=list)
    propagate: bool = Field(default=False)

    @field_validator("paths")
    @classmethod
    def validate_paths(cls, v: list[str]) -> list[str]:
        if not all(s.startswith("^") and s.endswith("$") for s in v):
            raise ValueError("路径必须以 ^ 开头，以 $ 结尾")
        return v

    @field_validator("excludes")
    @classmethod
    def validate_excludes(cls, v: list[str]) -> list[str]:
        if not all(s.startswith("^") and s.endswith("$") for s in v):
            raise ValueError("路径必须以 ^ 开头，以 $ 结尾")
        return v

    @property
    def display_name(self) -> str:
        return f"source[{len(self.paths)}]:{self.paths[0]}"

    @cached_property
    def checksum_value(self) -> str:
        matched_files = self.get_matched_files_with_checksum()
        return hashlib.sha256(repr(matched_files).encode("utf-8")).hexdigest()

    def get_matched_files(self) -> list[str]:
        """找出在 git 管理中，且符合条件的文件列表"""
        path_patterns = [re.compile(p) for p in self.paths]
        exclude_patterns = [re.compile(p) for p in self.excludes]

        cmd = ["git", "ls-files"]
        # 开发机环境下，同时计算未加入到 git 管理的文件
        if not CIEnvironment().is_ci:
            cmd.extend(["-co", "--exclude-standard"])

        tracked_files = subprocess.check_output(cmd, text=True).splitlines()

        matched_files = set()
        matched_counts = defaultdict(int)
        for f in tracked_files:
            if not os.path.exists(f):
                continue

            matched = False
            for p in path_patterns:
                if p.match(f):
                    matched_counts[p.pattern] += 1
                    matched = True
                    break
            for p in exclude_patterns:
                if p.match(f):
                    matched_counts[p.pattern] += 1
                    matched = False
                    break
            if matched:
                matched_files.add(f)

        for p in chain(path_patterns, exclude_patterns):
            if matched_counts[p.pattern] == 0:
                raise ValueError(f"配置文件有误：没有找到匹配的文件: {p.pattern}")

        return sorted(matched_files)

    def get_matched_files_with_checksum(self) -> list[tuple[str, str]]:
        paths = self.get_matched_files()
        return [(p, generate_sha256sum(p)) for p in paths]


class DependencyTypeCommand(BaseModel):
    type: Literal["command"]
    commands: list[Command]
    propagate: bool = Field(default=False)

    @property
    def display_name(self) -> str:
        return f"command[{len(self.commands)}]:{self.commands[0].command.split()[0]}"

    @cached_property
    def checksum_value(self) -> str:
        outputs = self.get_command_outputs()
        logger.debug(f"- 命令依赖：{outputs}")
        sha256_hash = hashlib.sha256()
        sha256_hash.update(repr(outputs).encode("utf-8"))
        return sha256_hash.hexdigest()

    def get_command_outputs(self) -> list[tuple[str, str]]:
        outputs = []
        for command in self.commands:
            output = subprocess.check_output(command.command, cwd=command.path, text=True, shell=True)
            outputs.append((command.command, output))
        return outputs


class DependencyTypeVariable(BaseModel):
    type: Literal["variable"]
    names: list[str]
    excludes: list[str] = Field(default_factory=list)
    propagate: bool = Field(default=False)

    @field_validator("names")
    @classmethod
    def validate_names(cls, v: list[str]) -> list[str]:
        if not all(s.startswith("^") and s.endswith("$") for s in v):
            raise ValueError("环境变量名必须以 ^ 开头，以 $ 结尾")
        return v

    @property
    def display_name(self) -> str:
        return f"variable[{len(self.names)}]:{self.names[0]}"

    @cached_property
    def checksum_value(self) -> str:
        sha256_hash = hashlib.sha256()
        matched_variables = self.get_matched_variables()
        logger.debug(f"- 环境变量依赖：{[x[0] for x in matched_variables]}")
        sha256_hash.update(repr(matched_variables).encode("utf-8"))
        return sha256_hash.hexdigest()

    def get_matched_variables(self) -> list[tuple[str, str]]:
        name_patterns = [re.compile(p) for p in self.names]
        exclude_patterns = [re.compile(p) for p in self.excludes]

        matched_variables = []
        for k, v in os.environ.items():
            matched = False
            for p in name_patterns:
                if p.match(k):
                    matched = True
                    break
            for p in exclude_patterns:
                if p.match(k):
                    matched = False
                    break
            if matched:
                matched_variables.append((k, v))

        return sorted(matched_variables, key=lambda x: x[0])


class DependencyTypeTarget(BaseModel):
    type: Literal["target"]
    name: str
    propagate: bool = Field(default=False)

    @property
    def display_name(self) -> str:
        return f"target:{self.name}"

    @property
    def target(self):
        from quack.spec import Spec

        if self.name not in Spec.get().targets:
            logger.critical(f"未找到 Target {self.name}")
            sys.exit(1)

        return Spec.get().targets[self.name]

    @cached_property
    def checksum_value(self) -> str:
        return self.target.checksum_value


class DependencyTypeGlobal(BaseModel):
    type: Literal["global"]
    name: str
    propagate: bool = Field(default=False)

    @property
    def display_name(self) -> str:
        return f"global:{self.name}"

    @cached_property
    def checksum_value(self) -> str:
        raise NotImplementedError


DependencyType = (
    DependencyTypeCommand | DependencyTypeGlobal | DependencyTypeSource | DependencyTypeTarget | DependencyTypeVariable
)

Dependency = Annotated[DependencyType, Field(discriminator="type")]
