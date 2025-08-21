#!/usr/bin/env python3

import hashlib
import os
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from itertools import chain

from loguru import logger

from quack.exceptions import SpecError
from quack.models.command import Command
from quack.utils.checksummer import generate_sha256sum


@dataclass
class DependencyTypeSource:
    paths: list[str]
    excludes: list[str]

    def __init__(self, data) -> None:
        self.paths = data["paths"]
        self.excludes = data.get("excludes", [])
        self._checksum_value = None

    @property
    def display_name(self) -> str:
        return f"source[{len(self.paths)}]:{self.paths[0]}"

    def validate(self) -> None:
        for path in chain(self.paths, self.excludes):
            if not path.startswith("^") or not path.endswith("$"):
                raise SpecError(f"路径必须以 ^ 开头，以 $ 结尾：{path}")

    @property
    def checksum_value(self) -> str:
        if self._checksum_value is None:
            self._checksum_value = self.compute_checksum()
        return self._checksum_value

    def get_matched_files(self) -> list[str]:
        """找出在 git 管理中，且符合条件的文件列表"""
        from quack.runtime import RuntimeState

        path_patterns = [re.compile(p) for p in self.paths]
        exclude_patterns = [re.compile(p) for p in self.excludes]

        cmd = ["git", "ls-files"]
        # 开发机环境下，同时计算未加入到 git 管理的文件
        if not RuntimeState.get().is_ci:
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
                raise SpecError(f"配置文件有误：没有找到匹配的文件: {p.pattern}")

        return sorted(list(matched_files))

    def get_matched_files_with_checksum(self) -> list[tuple[str, str]]:
        paths = self.get_matched_files()
        result = []
        for p in paths:
            result.append((p, generate_sha256sum(p)))
        return result

    def compute_checksum(self) -> str:
        matched_files = self.get_matched_files_with_checksum()
        return hashlib.sha256(repr(matched_files).encode("utf-8")).hexdigest()


@dataclass
class DependencyTypeCommand:
    commands: list[Command]

    def __init__(self, data) -> None:
        self.commands = []
        for c in data["commands"]:
            if isinstance(c, str):
                self.commands.append(Command({"command": c}))
            else:
                self.commands.append(Command(c))
        self._checksum_value = None

    @property
    def display_name(self) -> str:
        return f"command[{len(self.commands)}]:{self.commands[0].command.split()[0]}"

    def validate(self) -> None:
        for command in self.commands:
            command.validate()

    @property
    def checksum_value(self) -> str:
        if self._checksum_value is None:
            self._checksum_value = self.compute_checksum()
        return self._checksum_value

    def get_command_outputs(self) -> list[tuple[str, str]]:
        outputs = []
        for command in self.commands:
            output = subprocess.check_output(
                command.command, cwd=command.path, text=True, shell=True
            )
            outputs.append((command.command, output))
        return outputs

    def compute_checksum(self) -> str:
        outputs = self.get_command_outputs()
        logger.debug(f"- 命令依赖：{outputs}")
        sha256_hash = hashlib.sha256()
        sha256_hash.update(repr(outputs).encode("utf-8"))
        return sha256_hash.hexdigest()


@dataclass
class DependencyTypeVariable:
    names: list[str]
    excludes: list[str]

    def __init__(self, data) -> None:
        self.names = data["names"]
        self.excludes = data.get("excludes", [])
        self._checksum_value = None

    @property
    def display_name(self) -> str:
        return f"variable[{len(self.names)}]:{self.names[0]}"

    def validate(self) -> None:
        for name in chain(self.names, self.excludes):
            if not name.startswith("^") or not name.endswith("$"):
                raise SpecError(f"环境变量名必须以 ^ 开头，以 $ 结尾：{name}")

    @property
    def checksum_value(self) -> str:
        if self._checksum_value is None:
            self._checksum_value = self.compute_checksum()
        return self._checksum_value

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

    def compute_checksum(self) -> str:
        sha256_hash = hashlib.sha256()
        matched_variables = self.get_matched_variables()
        logger.debug(f"- 环境变量依赖：{[x[0] for x in matched_variables]}")
        sha256_hash.update(repr(matched_variables).encode("utf-8"))
        return sha256_hash.hexdigest()


@dataclass
class DependencyTypeTarget:
    name: str

    def __init__(self, data) -> None:
        self.name = data["name"]
        self._checksum_value = None

    @property
    def display_name(self) -> str:
        return f"target:{self.name}"

    def validate(self) -> None:
        from quack.spec import Spec

        if self.name not in Spec.get().targets:
            raise SpecError(f"Target {self.name} 不存在")

    @property
    def target(self):
        from quack.spec import Spec

        return Spec.get().targets[self.name]

    @property
    def checksum_value(self) -> str:
        if self._checksum_value is None:
            self._checksum_value = self.compute_checksum()
        return self._checksum_value

    def compute_checksum(self) -> str:
        return self.target.checksum_value


DependencyType = (
    DependencyTypeCommand
    | DependencyTypeSource
    | DependencyTypeTarget
    | DependencyTypeVariable
)
