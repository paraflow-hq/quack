import hashlib
from unittest import mock

import pytest

from quack.exceptions import SpecError
from quack.models.dependency import (
    DependencyTypeCommand,
    DependencyTypeSource,
    DependencyTypeTarget,
    DependencyTypeVariable,
)


class TestDependencyTypeSource:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeSource(
            {
                "paths": [
                    r"^src/quack/__init__.py$",
                    r"^README.md$",
                ],
                "excludes": [
                    r"^README.md$",
                ],
            }
        )

    def test_validate(self):
        d = DependencyTypeSource({"paths": ["^$"], "excludes": ["^$"]})
        d.validate()

        d = DependencyTypeSource({"paths": ["^"], "excludes": ["^$"]})
        with pytest.raises(SpecError) as exc_info:
            d.validate()
        assert "以 $ 结尾" in str(exc_info.value)

        d = DependencyTypeSource({"paths": ["^$"], "excludes": ["$"]})
        with pytest.raises(SpecError) as exc_info:
            d.validate()
        assert "以 ^ 开头" in str(exc_info.value)

    @mock.patch("quack.runtime.RuntimeState")
    def test_get_matched_files(self, mock_runtime, mock_dependency):
        """测试文件匹配，包括 git 管理的文件和未管理的文件"""
        # CI 环境下只考虑 git 管理的文件
        mock_runtime.get.return_value.is_ci = True
        assert mock_dependency.get_matched_files() == ["src/quack/__init__.py"]

        # 非 CI 环境下，同时考虑未加入 git 管理的文件
        mock_runtime.get.return_value.is_ci = False
        assert mock_dependency.get_matched_files() == ["src/quack/__init__.py"]

        # 测试 excludes 为空的情况
        mock_dependency.excludes = []
        assert mock_dependency.get_matched_files() == [
            "README.md",
            "src/quack/__init__.py",
        ]

    @mock.patch("quack.runtime.RuntimeState")
    @mock.patch("subprocess.check_output")
    def test_get_matched_files_with_untracked(self, mock_check_output, mock_runtime):
        """测试非 CI 环境下包含未跟踪的文件"""
        mock_runtime.get.return_value.is_ci = False
        mock_check_output.return_value = (
            "src/quack/__init__.py\nREADME.md\nsrc/quack/new_file.py\n"
        )

        d = DependencyTypeSource(
            {
                "paths": [
                    r"^src/quack/.*\.py$",
                ],
            }
        )

        with mock.patch("os.path.exists", lambda x: x.endswith(".py")):
            assert d.get_matched_files() == [
                "src/quack/__init__.py",
                "src/quack/new_file.py",
            ]

    @mock.patch("quack.runtime.RuntimeState")
    @mock.patch("subprocess.check_output")
    def test_get_matched_files_with_deleted(self, mock_check_output, mock_runtime):
        """测试非 CI 环境下处理已删除的文件"""
        mock_runtime.get.return_value.is_ci = False
        mock_check_output.return_value = (
            "src/quack/__init__.py\nscripts/quack/deleted.py\n"
        )

        d = DependencyTypeSource(
            {
                "paths": [
                    r"^src/quack/.*\.py$",
                ],
            }
        )

        # 模拟 deleted.py 文件不存在
        with mock.patch("os.path.exists", lambda x: x != "src/quack/deleted.py"):
            assert d.get_matched_files() == ["src/quack/__init__.py"]

    @mock.patch("quack.runtime.RuntimeState")
    def test_checksum_value(self, mock_runtime, mock_dependency):
        mock_runtime.get.return_value.is_ci = True
        result = [("src/quack/__init__.py", hashlib.sha256().hexdigest())]
        assert (
            mock_dependency.checksum_value
            == hashlib.sha256(repr(result).encode("utf-8")).hexdigest()
        )


class TestDependencyTypeCommand:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeCommand(
            {
                "commands": [
                    "echo -n 1",
                    "echo -n 2",
                    {
                        "command": "echo -n 3",
                        "path": "src/quack",
                    },
                ],
            }
        )

    def test_init(self, mock_dependency):
        assert mock_dependency.commands[0].command == "echo -n 1"

    def test_checksum_value(self, mock_dependency):
        result = [
            ("echo -n 1", "1"),
            ("echo -n 2", "2"),
            ("echo -n 3", "3"),
        ]
        assert (
            mock_dependency.checksum_value
            == hashlib.sha256(repr(result).encode("utf-8")).hexdigest()
        )


class TestDependencyTypeVariable:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeVariable(
            {
                "names": [
                    r"^QUACK_MOCK_.*$",
                ],
                "excludes": [
                    r"^QUACK_MOCK_LOG_.*$",
                ],
            }
        )

    def test_validate(self):
        d = DependencyTypeVariable({"names": ["^$"], "excludes": ["^$"]})
        d.validate()

        d = DependencyTypeVariable({"names": ["^"], "excludes": ["^$"]})
        with pytest.raises(SpecError) as exc_info:
            d.validate()
        assert "以 $ 结尾" in str(exc_info.value)

        d = DependencyTypeVariable({"names": ["^$"], "excludes": ["$"]})
        with pytest.raises(SpecError) as exc_info:
            d.validate()
        assert "以 ^ 开头" in str(exc_info.value)

    def test_get_matched_variables(self, mock_dependency, monkeypatch):
        monkeypatch.setenv("QUACK_MOCK_DEBUG", "1")
        monkeypatch.setenv("QUACK_MOCK_CI_ENVIRONMENT", "testing")
        monkeypatch.setenv("QUACK_MOCK_LOG_LEVEL", "INFO")
        assert mock_dependency.get_matched_variables() == [
            ("QUACK_MOCK_CI_ENVIRONMENT", "testing"),
            ("QUACK_MOCK_DEBUG", "1"),
        ]

    def test_checksum_value(self, mock_dependency, monkeypatch):
        monkeypatch.setenv("QUACK_MOCK_DEBUG", "1")
        monkeypatch.setenv("QUACK_MOCK_CI_ENVIRONMENT", "testing")
        monkeypatch.setenv("QUACK_MOCK_LOG_LEVEL", "INFO")
        result = [("QUACK_MOCK_CI_ENVIRONMENT", "testing"), ("QUACK_MOCK_DEBUG", "1")]
        assert (
            mock_dependency.checksum_value
            == hashlib.sha256(repr(result).encode("utf-8")).hexdigest()
        )


class TestDependencyTypeTarget:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeTarget(
            {
                "name": "abc",
            }
        )

    def test_validate(self, mock_dependency):
        mock_dependency = DependencyTypeTarget({"name": "quack"})
        mock_dependency.validate()

        mock_dependency = DependencyTypeTarget({"name": "not-found"})
        with pytest.raises(SpecError) as exc_info:
            mock_dependency.validate()
        assert "不存在" in str(exc_info.value)
