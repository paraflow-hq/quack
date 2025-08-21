#!/usr/bin/env python3

import os
from pathlib import Path

from quack.runtime import RuntimeState


class TestRuntimeState:
    def test_init(self):
        """测试使用空环境初始化"""
        runtime = RuntimeState(Path.cwd(), {}, "dev", None)
        assert not runtime.is_ci
        assert not runtime.is_nested
        assert runtime.cache == "dev"

    def test_init_with_nested_env(self):
        """测试嵌套环境初始化"""
        runtime = RuntimeState(Path.cwd(), {"QUACK_NESTED": "true"}, "dev", None)
        assert not runtime.is_ci
        assert runtime.is_nested

    def test_init_with_test_features(self):
        """测试使用测试特性初始化"""
        runtime = RuntimeState(Path.cwd(), {}, "dev", "on")
        assert runtime.test_features == "true"
        runtime = RuntimeState(Path.cwd(), {}, "dev", "off")
        assert runtime.test_features == "false"

    def test_setup(self):
        """测试环境变量设置"""
        runtime = RuntimeState(Path.cwd(), {}, "dev", None)
        runtime.setup()

        assert os.environ["QUACK_NESTED"] == "true"
        assert os.environ["QUACK_CACHE"] == "dev"
