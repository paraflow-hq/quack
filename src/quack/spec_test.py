#!/usr/bin/env python3

import os
from pathlib import Path

import pytest

from quack.exceptions import SpecError
from quack.spec import Spec


class TestSpec:
    def setup_method(self):
        self.pwd = Path(os.getcwd())
        self.path = Path("quack.yaml")

    def test_target_name_duplicate(self):
        data = {
            "app_name": "quack_test",
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

        with pytest.raises(SpecError) as exc_info:
            Spec(self.pwd, self.path, data)
        assert "名称重复" in str(exc_info.value)

    def test_script_name_duplicate(self):
        data = {
            "app_name": "quack_test",
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

        with pytest.raises(SpecError) as exc_info:
            Spec(self.pwd, self.path, data)
        assert "名称重复" in str(exc_info.value)

    def test_script_target_name_conflict(self):
        data = {
            "app_name": "quack_test",
            "targets": [
                {
                    "name": "test",
                    "description": "my test target",
                    "dependencies": [],
                    "outputs": {"paths": ["/tmp/test-output"]},
                    "operations": {
                        "build": {"command": "echo hello > /tmp/test-output"}
                    },
                }
            ],
            "scripts": [
                {
                    "name": "test",
                    "description": "my test script",
                    "command": {"command": "echo test"},
                }
            ],
        }

        with pytest.raises(SpecError) as exc_info:
            Spec(self.pwd, self.path, data)
        assert "名称重复" in str(exc_info.value)
