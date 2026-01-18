import hashlib
from unittest import mock

import pytest
from pydantic import ValidationError

from quack.models.dependency import (
    DependencyTypeCommand,
    DependencyTypeSource,
    DependencyTypeVariable,
)


@pytest.fixture(autouse=True)
def clear_git_ls_files_cache():
    """自动清除 git ls-files 缓存"""
    DependencyTypeSource._get_git_ls_files.cache_clear()


class TestDependencyTypeSource:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeSource.model_validate(
            {
                "type": "source",
                "paths": [
                    r"^src/quack/__init__.py$",
                    r"^README.md$",
                ],
                "excludes": [
                    r"^README.md$",
                ],
            }
        )

    def test_validation(self):
        # 测试有效的路径
        DependencyTypeSource.model_validate({"type": "source", "paths": ["^$"], "excludes": ["^$"]})

        # 测试无效的 paths 格式（不以 $ 结尾）
        with pytest.raises(ValidationError) as exc_info:
            DependencyTypeSource.model_validate({"type": "source", "paths": ["^"], "excludes": ["^$"]})
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "路径必须以 ^ 开头，以 $ 结尾" in str(errors[0]["msg"])

        # 测试无效的 excludes 格式（不以 ^ 开头）
        with pytest.raises(ValidationError) as exc_info:
            DependencyTypeSource.model_validate({"type": "source", "paths": ["^$"], "excludes": ["$"]})
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "路径必须以 ^ 开头，以 $ 结尾" in str(errors[0]["msg"])

    @mock.patch("quack.utils.ci_environment.CIEnvironment")
    def test_get_matched_files(self, mock_ci_environment, mock_dependency):
        """测试文件匹配，包括 git 管理的文件和未管理的文件"""
        # CI 环境下只考虑 git 管理的文件
        mock_ci_environment.return_value.is_ci = True
        assert mock_dependency.get_matched_files() == ["src/quack/__init__.py"]

        # 非 CI 环境下，同时考虑未加入 git 管理的文件
        mock_ci_environment.return_value.is_ci = False
        assert mock_dependency.get_matched_files() == ["src/quack/__init__.py"]

        # 测试 excludes 为空的情况
        mock_dependency.excludes = []
        assert mock_dependency.get_matched_files() == [
            "README.md",
            "src/quack/__init__.py",
        ]

    @mock.patch("quack.utils.ci_environment.CIEnvironment")
    @mock.patch("subprocess.check_output")
    def test_get_matched_files_with_untracked(self, mock_check_output, mock_ci_environment):
        """测试非 CI 环境下包含未跟踪的文件"""
        mock_ci_environment.return_value.is_ci = False
        mock_check_output.return_value = "src/quack/__init__.py\nREADME.md\nsrc/quack/new_file.py\n"

        d = DependencyTypeSource.model_validate(
            {
                "type": "source",
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

    @mock.patch("quack.utils.ci_environment.CIEnvironment")
    @mock.patch("subprocess.check_output")
    def test_get_matched_files_with_deleted(self, mock_check_output, mock_ci_environment):
        """测试非 CI 环境下处理已删除的文件"""
        mock_ci_environment.return_value.is_ci = False
        mock_check_output.return_value = "src/quack/__init__.py\nscripts/quack/deleted.py\n"

        d = DependencyTypeSource.model_validate(
            {
                "type": "source",
                "paths": [
                    r"^src/quack/.*\.py$",
                ],
            }
        )

        # 模拟 deleted.py 文件不存在
        with mock.patch("os.path.exists", lambda x: x != "src/quack/deleted.py"):
            assert d.get_matched_files() == ["src/quack/__init__.py"]

    @mock.patch("quack.utils.ci_environment.CIEnvironment")
    def test_checksum_value(self, mock_ci_environment, mock_dependency):
        mock_ci_environment.return_value.is_ci = True
        result = [("src/quack/__init__.py", hashlib.sha256().hexdigest())]
        assert mock_dependency.checksum_value == hashlib.sha256(repr(result).encode("utf-8")).hexdigest()


class TestDependencyTypeCommand:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeCommand.model_validate(
            {
                "type": "command",
                "commands": [
                    "printf '1'",
                    "printf '2'",
                    {
                        "command": "printf '3'",
                        "path": "src/quack",
                    },
                ],
            }
        )

    def test_init(self, mock_dependency):
        assert mock_dependency.commands[0].command == "printf '1'"

    def test_checksum_value(self, mock_dependency):
        result = [
            ("printf '1'", "1"),
            ("printf '2'", "2"),
            ("printf '3'", "3"),
        ]
        assert mock_dependency.checksum_value == hashlib.sha256(repr(result).encode("utf-8")).hexdigest()


class TestDependencyTypeVariable:
    @pytest.fixture(scope="function")
    def mock_dependency(self):
        return DependencyTypeVariable.model_validate(
            {
                "type": "variable",
                "names": [
                    r"^QUACK_MOCK_.*$",
                ],
                "excludes": [
                    r"^QUACK_MOCK_LOG_.*$",
                ],
            }
        )

    def test_validation(self):
        # 测试有效的环境变量名格式
        DependencyTypeVariable.model_validate({"type": "variable", "names": ["^$"], "excludes": ["^$"]})

        # 测试无效的 names 格式（不以 $ 结尾）
        with pytest.raises(ValidationError) as exc_info:
            DependencyTypeVariable.model_validate({"type": "variable", "names": ["^"], "excludes": ["^$"]})
        assert "环境变量名必须以 ^ 开头，以 $ 结尾" in str(exc_info.value)

        # 测试缺少必要字段
        with pytest.raises(ValidationError):
            DependencyTypeVariable.model_validate({"type": "variable"})

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
        assert mock_dependency.checksum_value == hashlib.sha256(repr(result).encode("utf-8")).hexdigest()
