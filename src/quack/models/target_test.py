from typing import ClassVar, cast
from unittest import mock

import pytest
from pydantic import ValidationError

from quack.cache import TargetCacheBackendType
from quack.config import Config
from quack.exceptions import CacheCorruptionError, CloudStorageError, CloudStorageTransientError
from quack.models.target import Target, TargetExecutionMode


class RecordingCacheBackend:
    NAME: ClassVar[str] = "recording"
    exists_result: ClassVar[bool] = False
    exists_error: ClassVar[Exception | None] = None
    load_error: ClassVar[Exception | None] = None
    save_error: ClassVar[Exception | None] = None
    exists_calls: ClassVar[int] = 0
    load_calls: ClassVar[int] = 0
    save_calls: ClassVar[int] = 0

    def __init__(self, _config: Config, _app_name: str) -> None:
        pass

    def exists(self, _target: Target) -> bool:
        type(self).exists_calls += 1
        error = type(self).exists_error
        if error is not None:
            raise error
        return type(self).exists_result

    def load(self, _target: Target) -> None:
        type(self).load_calls += 1
        error = type(self).load_error
        if error is not None:
            raise error

    def save(self, _target: Target) -> None:
        type(self).save_calls += 1
        error = type(self).save_error
        if error is not None:
            raise error


def reset_recording_cache_backend(
    *,
    exists_result: bool = False,
    exists_error: Exception | None = None,
    load_error: Exception | None = None,
    save_error: Exception | None = None,
) -> type[TargetCacheBackendType]:
    RecordingCacheBackend.exists_result = exists_result
    RecordingCacheBackend.exists_error = exists_error
    RecordingCacheBackend.load_error = load_error
    RecordingCacheBackend.save_error = save_error
    RecordingCacheBackend.exists_calls = 0
    RecordingCacheBackend.load_calls = 0
    RecordingCacheBackend.save_calls = 0
    return cast(type[TargetCacheBackendType], RecordingCacheBackend)


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

    def test_execute_deps_only(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend()

        # 当 mode=TargetExecutionMode.DEPS_ONLY 仅构建依赖项
        target.execute(
            config,
            mock_test_spec.app_name,
            cache_backend,
            mode=TargetExecutionMode.DEPS_ONLY,
        )

        assert RecordingCacheBackend.exists_calls == 0
        assert RecordingCacheBackend.load_calls == 0
        assert RecordingCacheBackend.save_calls == 0

    def test_execute_cache_hit(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(exists_result=True)

        # 当缓存命中时，直接加载缓存
        target.execute(config, mock_test_spec.app_name, cache_backend)
        assert RecordingCacheBackend.load_calls == 1

    def test_execute_rebuilds_after_transient_cache_hit_failure(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(
            exists_error=CloudStorageTransientError(
                "检查文件是否存在失败",
                "Could not connect to the endpoint URL",
            )
        )

        with mock.patch("quack.models.command.Command.execute") as mock_build:
            target.execute(config, mock_test_spec.app_name, cache_backend)

        mock_build.assert_called_once()
        assert RecordingCacheBackend.load_calls == 0
        assert RecordingCacheBackend.save_calls == 1

    def test_execute_rebuilds_after_transient_cache_load_failure(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(
            exists_result=True,
            load_error=CloudStorageTransientError(
                "下载文件失败",
                "IncompleteBody",
                code="IncompleteBody",
            ),
        )

        with mock.patch("quack.models.command.Command.execute") as mock_build:
            target.execute(config, mock_test_spec.app_name, cache_backend)

        assert RecordingCacheBackend.load_calls == 1
        mock_build.assert_called_once()
        assert RecordingCacheBackend.save_calls == 1

    def test_execute_rebuilds_after_corrupt_cache_load(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(
            exists_result=True,
            load_error=CacheCorruptionError("缓存归档解压失败"),
        )

        with mock.patch("quack.models.command.Command.execute") as mock_build:
            target.execute(config, mock_test_spec.app_name, cache_backend)

        assert RecordingCacheBackend.load_calls == 1
        mock_build.assert_called_once()
        assert RecordingCacheBackend.save_calls == 1

    def test_execute_raises_non_transient_cache_load_failure(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(
            exists_result=True,
            load_error=CloudStorageError(
                "下载文件失败",
                "AccessDenied",
                code="AccessDenied",
            ),
        )

        with mock.patch("quack.models.command.Command.execute") as mock_build, pytest.raises(CloudStorageError):
            target.execute(config, mock_test_spec.app_name, cache_backend)

        mock_build.assert_not_called()
        assert RecordingCacheBackend.save_calls == 0

    def test_execute_ignores_transient_cache_save_failure(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(
            exists_result=False,
            save_error=CloudStorageTransientError(
                "上传文件失败",
                "IncompleteBody",
                code="IncompleteBody",
            ),
        )

        with mock.patch("quack.models.command.Command.execute") as mock_build:
            target.execute(config, mock_test_spec.app_name, cache_backend)

        mock_build.assert_called_once()
        assert RecordingCacheBackend.save_calls == 1

    def test_execute_raises_non_transient_cache_save_failure(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(
            exists_result=False,
            save_error=CloudStorageError(
                "上传文件失败",
                "AccessDenied",
                code="AccessDenied",
            ),
        )

        with mock.patch("quack.models.command.Command.execute"), pytest.raises(CloudStorageError):
            target.execute(config, mock_test_spec.app_name, cache_backend)

    def test_execute_load_only(self, mock_test_spec: mock.Mock):
        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        cache_backend = reset_recording_cache_backend(exists_result=False)

        # 当 mode=TargetExecutionMode.LOAD_ONLY 且缓存未命中时，应该退出
        with pytest.raises(SystemExit):
            target.execute(
                config,
                mock_test_spec.app_name,
                cache_backend,
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
