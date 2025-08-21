from unittest import mock

import pytest

from quack.config import Config
from quack.exceptions import SpecError
from quack.models.target import Target, TargetExecutionMode
from quack.spec import Spec


class TestTarget:
    def test_init(self):
        with pytest.raises(SpecError) as exc_info:
            Target(
                {
                    "name": "abc",
                    "description": "abc abc",
                    "dependencies": [
                        {
                            "type": "unknown",
                        },
                    ],
                    "outputs": {
                        "paths": [],
                    },
                    "operations": {
                        "build": {
                            "command": "",
                        },
                    },
                }
            )
        assert "未知类型" in str(exc_info.value)

    def test_cache_path(self):
        assert Spec.get().targets["quack"].cache_path.startswith("quack/")

    def test_cache_archive_filename(self):
        assert Spec.get().targets["quack"].cache_archive_filename == "quack.tar.gz"

    @mock.patch("quack.cache.TargetCache")
    def test_execute_deps_only(self, mock_target_cache):
        config = Config.model_construct()
        target = Spec.get().targets["quack"]
        target._checksum_value = ""

        # 当 mode=TargetExecutionMode.DEPS_ONLY 仅构建依赖项
        mock_target_cache.return_value.hit.return_value = False
        target.execute(config, mock.Mock, mode=TargetExecutionMode.DEPS_ONLY)

    @mock.patch("quack.cache.TargetCache")
    def test_execute_cache_hit(self, mock_target_cache):
        config = Config.model_construct()
        target = Spec.get().targets["quack"]
        target._checksum_value = ""

        # 当缓存命中时，直接加载缓存
        mock_target_cache.return_value.hit.return_value = True
        target.execute(config, mock.Mock)
        mock_target_cache.return_value.load.assert_called_once()

    @mock.patch("quack.cache.TargetCache")
    def test_execute_load_only(self, mock_target_cache):
        config = Config.model_construct()
        target = Spec.get().targets["quack"]
        target._checksum_value = ""

        # 当 mode=TargetExecutionMode.LOAD_ONLY 且缓存未命中时，应该退出
        mock_target_cache.return_value.hit.return_value = False
        with pytest.raises(SystemExit):
            target.execute(config, mock.Mock, mode=TargetExecutionMode.LOAD_ONLY)

    def test_inherit_outputs_from_target_dependencies(self):
        """测试继承目标依赖的输出路径功能"""
        # 创建一个依赖目标，有输出路径
        dep_target_data = {
            "name": "target:base",
            "description": "基础目标",
            "dependencies": [],
            "outputs": {"paths": ["dist/base.js", "dist/base.css"]},
            "operations": {"build": {"command": "echo 'build base'"}},
        }

        # 创建继承输出的目标
        inherit_target_data = {
            "name": "target:inherit",
            "description": "继承输出的目标",
            "dependencies": [{"type": "target", "name": "target:base"}],
            "outputs": {"inherit": True, "paths": ["dist/inherit.js"]},
            "operations": {"build": {"command": "echo 'build inherit'"}},
        }

        # 创建目标实例
        base_target = Target(dep_target_data)
        Spec.get().add_target(base_target)
        inherit_target = Target(inherit_target_data)

        # 验证继承的输出路径
        expected_paths = {"dist/inherit.js", "dist/base.js", "dist/base.css"}
        assert inherit_target.outputs.paths == expected_paths
