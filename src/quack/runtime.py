#!/usr/bin/env python3

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar


@dataclass
class RuntimeState:
    """管理环境变量和命令行参数等运行时状态"""

    _INSTANCE: ClassVar[RuntimeState | None] = None

    is_ci: bool
    is_nested: bool
    cache: str
    test_features: str

    def __init__(
        self,
        pwd: Path,
        environ: dict[str, str],
        cache: str,
        args_test_features: str | None,
    ) -> None:
        if RuntimeState._INSTANCE is None:
            RuntimeState._INSTANCE = self

        self.is_ci = environ.get("CI") == "true"
        self.is_nested = environ.get("QUACK_NESTED") in ["1", "true"]
        self.cache = cache
        self.test_features = (
            "true"
            if (
                args_test_features == "on"
                or os.environ.get("WK_INTEGRATION_TEST_ENABLE_FEATURE_BY_DEFAULT")
                == "true"
            )
            else "false"
        )

    def setup(self):
        """为子进程设置环境变量"""
        # NOTE: 这里加了新变量，可能需要在 quack.yaml 里面忽略掉，否则缓存可能失效
        os.environ["QUACK_NESTED"] = "true"
        os.environ["QUACK_CACHE"] = self.cache
        os.environ["WK_INTEGRATION_TEST_ENABLE_FEATURE_BY_DEFAULT"] = self.test_features

    @classmethod
    def get(cls) -> RuntimeState:
        assert cls._INSTANCE is not None
        return cls._INSTANCE
