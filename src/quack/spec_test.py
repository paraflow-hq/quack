#!/usr/bin/env python3

import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from quack.spec import Spec


class TestSpec:
    def setup_method(self):
        self.cwd = Path(os.getcwd())
        self.path = Path("quack.yaml")

    def test_target_name_duplicate(self):
        data = {
            "app_name": "quack_test",
            "cwd": str(self.cwd.resolve()),
            "path": str(self.path.resolve()),
            "global_dependencies": [],
            "targets": [
                {
                    "name": "quack",
                    "description": "my test target",
                    "dependencies": [],
                    "outputs": {"paths": ["/tmp/quack-output"]},
                    "operations": {
                        "build": {"command": "echo hello > /tmp/quack-output"}
                    },
                },
                {
                    "name": "quack",
                    "description": "my test target",
                    "dependencies": [],
                    "outputs": {"paths": ["/tmp/quack-output"]},
                    "operations": {
                        "build": {"command": "echo hello > /tmp/quack-output"}
                    },
                },
            ],
        }

        with pytest.raises(ValidationError) as exc_info:
            Spec.model_validate(data)

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "名称重复" in str(errors[0]["msg"])

    def test_script_name_duplicate(self):
        data = {
            "app_name": "quack_test",
            "cwd": str(self.cwd.resolve()),
            "path": str(self.path.resolve()),
            "global_dependencies": [],
            "scripts": [
                {
                    "name": "test",
                    "description": "my test script",
                    "command": {"command": "echo test"},
                },
                {
                    "name": "test",
                    "description": "my test script",
                    "command": {"command": "echo test"},
                },
            ],
        }

        with pytest.raises(ValidationError) as exc_info:
            Spec.model_validate(data)

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "名称重复" in str(errors[0]["msg"])

    def test_app_name_validation(self):
        """测试 app_name 字段的验证"""
        # 测试无效的格式
        data = {
            "app_name": "Invalid-Name!",  # 包含非法字符
            "cwd": str(self.cwd.resolve()),
            "path": str(self.path.resolve()),
            "global_dependencies": [],
        }

        with pytest.raises(ValidationError) as exc_info:
            Spec.model_validate_json(json.dumps(data))

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert str(errors[0]["type"]) == "string_pattern_mismatch"

        # 测试过长的名称
        data["app_name"] = "a" * 33  # 超过32字符限制
        with pytest.raises(ValidationError) as exc_info:
            Spec.model_validate_json(json.dumps(data))

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert str(errors[0]["type"]) == "string_too_long"

    def test_global_dependencies_validation(self):
        """测试全局依赖的验证"""
        # 测试重复的全局依赖名称
        data = {
            "app_name": "quack_test",
            "cwd": str(self.cwd.resolve()),
            "path": str(self.path.resolve()),
            "global_dependencies": [
                {
                    "name": "source:test",
                    "type": "source",
                    "paths": ["^src/.*$"],
                },
                {
                    "name": "source:test",  # 重复的名称
                    "type": "source",
                    "paths": ["^tests/.*$"],
                },
            ],
        }

        with pytest.raises(ValidationError) as exc_info:
            Spec.model_validate(data)

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "名称重复" in str(errors[0]["msg"])
