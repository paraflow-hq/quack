#!/usr/bin/env python3

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from quack.models.command import Command


class TestCommand:
    @patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}, clear=True)
    def test_command_init(self):
        base_path = Path("/base")

        # 测试基本初始化
        command = Command(
            {"command": "echo test", "path": "tmp", "variables": {"TEST": "value"}},
            base_path=base_path,
        )
        assert command.command == "echo test"
        assert command.path == base_path / "tmp"
        assert command.variables["TEST"] == "value"
        assert command.variables["PATH"] == "/usr/bin:/bin"

        # 测试默认值
        command = Command({"command": "echo test"}, base_path=base_path)
        assert command.path == base_path
        assert command.variables["PATH"] == "/usr/bin:/bin"

        # 测试使用字符串初始化
        command = Command("echo test", base_path=base_path)
        assert command.command == "echo test"
        assert command.path == base_path
        assert command.variables["PATH"] == "/usr/bin:/bin"

        # 测试不传入 base_path
        command = Command("echo test")
        assert command.command == "echo test"
        assert command.path == Path(os.getcwd())
        assert command.variables["PATH"] == "/usr/bin:/bin"

    def test_command_validation(self):
        command = Command({"command": "echo test"})
        command.validate()  # 不应该抛出异常

    @patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}, clear=True)
    @patch("subprocess.Popen")
    def test_command_execute_success(self, mock_popen):
        # 模拟进程执行成功
        mock_process = MagicMock()
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        # 测试不带参数的执行
        command = Command(
            {"command": "echo hello", "path": "tmp", "variables": {"TEST": "value"}},
            base_path=Path("/base"),
        )
        command.execute()

        # 验证 Popen 调用
        expected_env = {
            "PATH": "/usr/bin:/bin",
            "TEST": "value",
        }
        mock_popen.assert_called_once_with(
            "echo hello",
            shell=True,
            cwd=Path("/base") / "tmp",
            env=expected_env,
            start_new_session=True,
        )
        assert command.process == mock_process

        # 重置 mock
        mock_popen.reset_mock()

        # 测试带参数的执行
        command.execute(["world", "--flag"])
        mock_popen.assert_called_once_with(
            "echo hello world --flag",
            shell=True,
            cwd=Path("/base") / "tmp",
            env=expected_env,
            start_new_session=True,
        )

    @patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}, clear=True)
    @patch("subprocess.Popen")
    def test_command_execute_failure(self, mock_popen):
        # 模拟进程执行失败
        mock_process = MagicMock()
        mock_process.wait.return_value = 1
        mock_popen.return_value = mock_process

        command = Command({"command": "false"}, base_path=Path("/base"))
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            command.execute()
        assert exc_info.value.returncode == 1

    @patch("os.killpg")
    @patch("os.getpgid")
    def test_command_terminate(self, mock_getpgid, mock_killpg):
        # 模拟进程
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_getpgid.return_value = 12345

        command = Command({"command": "sleep 100"}, base_path=Path("/base"))
        command.process = mock_process
        command.terminate()

        # 验证进程组终止
        mock_getpgid.assert_called_once_with(12345)
        mock_killpg.assert_called_once_with(12345, 15)

    @patch("os.killpg")
    def test_command_terminate_no_process(self, mock_killpg):
        command = Command({"command": "echo test"}, base_path=Path("/base"))
        command.terminate()  # 不应该抛出异常
        mock_killpg.assert_not_called()

    @patch("os.killpg")
    def test_command_terminate_process_not_found(self, mock_killpg):
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_killpg.side_effect = ProcessLookupError()

        command = Command({"command": "echo test"}, base_path=Path("/base"))
        command.process = mock_process
        command.terminate()  # 不应该抛出异常
