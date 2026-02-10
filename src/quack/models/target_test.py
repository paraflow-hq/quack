from unittest import mock

import pytest
from pydantic import ValidationError

from quack.config import Config
from quack.models.target import Target, TargetExecutionMode


class TestTarget:
    def test_init(self):
        with pytest.raises(ValidationError) as exc_info:
            Target.model_validate(
                {
                    "name": "abc:test",
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
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "union_tag_invalid"

    def test_cache_path(self, mock_test_spec: mock.Mock):
        assert mock_test_spec.targets["quack:test"].cache_path.startswith("quack:test/")

    def test_cache_archive_filename(self, mock_test_spec: mock.Mock):
        assert mock_test_spec.targets["quack:test"].cache_archive_filename == "quack:test.tar.zst"

    @mock.patch("quack.cache.TargetCache")
    def test_execute_deps_only(self, mock_target_cache, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""

        # 当 mode=TargetExecutionMode.DEPS_ONLY 仅构建依赖项
        mock_target_cache.return_value.hit.return_value = False
        target.execute(
            config,
            mock_test_spec.app_name,
            mock.Mock,
            mode=TargetExecutionMode.DEPS_ONLY,
        )

    @mock.patch("quack.cache.TargetCache")
    def test_execute_cache_hit(self, mock_target_cache, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""

        # 当缓存命中时，直接加载缓存
        mock_target_cache.return_value.hit.return_value = True
        target.execute(config, mock_test_spec.app_name, mock.Mock)
        mock_target_cache.return_value.load.assert_called_once()

    @mock.patch("quack.cache.TargetCache")
    def test_execute_load_only(self, mock_target_cache, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""

        # 当 mode=TargetExecutionMode.LOAD_ONLY 且缓存未命中时，应该退出
        mock_target_cache.return_value.hit.return_value = False
        with pytest.raises(SystemExit):
            target.execute(
                config,
                mock_test_spec.app_name,
                mock.Mock,
                mode=TargetExecutionMode.LOAD_ONLY,
            )

    def test_outputs_inheritance(self, mock_test_spec: mock.Mock):
        """测试 outputs 继承功能"""
        assert "/tmp/quack-output" in mock_test_spec.targets["quack:test:child"].outputs.paths
        assert len(mock_test_spec.targets["quack:test:child"].outputs.paths) == 2

        assert "/tmp/quack-output" not in mock_test_spec.targets["quack:test:child:no-inheritance"].outputs.paths
        assert len(mock_test_spec.targets["quack:test:child:no-inheritance"].outputs.paths) == 1

    def test_global_dependencies(self, mock_test_spec: mock.Mock):
        assert len(mock_test_spec.targets["quack:test"].dependencies) == 2
        assert mock_test_spec.targets["quack:test"].dependencies[0].type == "source"
        assert mock_test_spec.targets["quack:test"].dependencies[1].type == "command"
