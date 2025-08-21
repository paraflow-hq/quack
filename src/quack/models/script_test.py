#!/usr/bin/env python3

import pytest

from quack.exceptions import SpecError
from quack.models.script import Script


class TestScript:
    def test_script_name_validation(self):
        # 测试有效的脚本名称
        valid_names = ["test", "test-script", "test_script", "test123", ".test"]
        for name in valid_names:
            script = Script(
                {
                    "name": name,
                    "description": "test",
                    "command": {"command": "echo test"},
                }
            )
            script.validate()

        # 测试无效的脚本名称
        invalid_names = [
            "Test",  # 大写字母
            "test script",  # 空格
            "test@script",  # 特殊字符
            "TEST",  # 全大写
            "a" * 33,  # 超长名称
        ]
        for name in invalid_names:
            script = Script(
                {
                    "name": name,
                    "description": "test",
                    "command": {"command": "echo test"},
                }
            )
            with pytest.raises(SpecError) as exc_info:
                script.validate()
            if len(name) > 32:
                assert "名称过长" in str(exc_info.value)
            else:
                assert "名称应是由小写字母、数字" in str(exc_info.value)

    def test_script_command_validation(self):
        # 测试命令验证
        script = Script(
            {
                "name": "test",
                "description": "test",
                "command": {
                    "command": "echo test",
                    "path": "test_path",
                },
            }
        )
        script.validate()  # 不应该抛出异常

    def test_script_get_by_name(self):
        # 测试获取存在的脚本
        script = Script.get_by_name("test")
        assert script.name == "test"
        assert script.command.command == "echo test"

        # 测试获取不存在的脚本
        with pytest.raises(SystemExit):
            Script.get_by_name("nonexistent")
