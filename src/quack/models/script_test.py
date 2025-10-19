#!/usr/bin/env python3

from pathlib import Path

import pytest
from pydantic import ValidationError

from quack.models.script import Script


class TestScript:
    def test_script_name_validation(self):
        # 测试有效的脚本名称
        valid_names = ["test", "test-script", "test_script", "test123", ".test"]
        for name in valid_names:
            script = Script.model_validate(
                {
                    "name": name,
                    "description": "test",
                    "command": {"command": "echo test"},
                }
            )
            assert script.name == name

        # 测试无效的脚本名称
        invalid_names = [
            "Test",  # 大写字母
            "test script",  # 空格
            "test@script",  # 特殊字符
            "TEST",  # 全大写
            "a" * 33,  # 超长名称
        ]
        for name in invalid_names:
            with pytest.raises(ValidationError) as exc_info:
                Script.model_validate(
                    {
                        "name": name,
                        "description": "test",
                        "command": {"command": "echo test"},
                    }
                )

            errors = exc_info.value.errors()
            assert len(errors) == 1
            if len(name) > 32:
                assert errors[0]["type"] == "string_too_long"
            else:
                assert errors[0]["type"] == "string_pattern_mismatch"

    def test_script_command_validation(self):
        # 测试命令验证
        script = Script.model_validate(
            {
                "name": "test",
                "description": "test",
                "command": {"command": "echo test", "path": "test_path"},
                "base_path": "/base",
            }
        )
        assert script.command.command == "echo test"
        assert script.command.path == Path("test_path")
        assert script.command.base_path == script.base_path

        # 测试缺失命令的情况
        with pytest.raises(ValidationError) as exc_info:
            Script.model_validate({"name": "test", "description": "test"})

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "missing"
