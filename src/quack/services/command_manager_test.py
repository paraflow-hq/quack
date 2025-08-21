#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch

from quack.models.command import Command
from quack.services.command_manager import CommandManager


class CommandManagerTest(unittest.TestCase):
    def setUp(self):
        # 每个测试前重置单例
        CommandManager._instance = None
        self.manager = CommandManager.get()

    def test_singleton(self):
        """测试 CommandManager 的单例模式"""
        # 直接实例化应该抛出异常
        with self.assertRaises(RuntimeError):
            CommandManager()

        # 通过 get() 获取的实例应该是同一个
        manager1 = CommandManager.get()
        manager2 = CommandManager.get()
        self.assertIs(manager1, manager2)

    def test_register_and_unregister(self):
        """测试命令的注册和取消注册"""
        command = Mock(spec=Command)

        # 测试注册
        self.manager.register(command)
        self.assertIn(command, self.manager._active_commands)
        self.assertEqual(len(self.manager._active_commands), 1)

        # 测试取消注册
        self.manager.unregister(command)
        self.assertNotIn(command, self.manager._active_commands)
        self.assertEqual(len(self.manager._active_commands), 0)

        # 测试取消注册不存在的命令
        self.manager.unregister(command)  # 不应该抛出异常
        self.assertEqual(len(self.manager._active_commands), 0)

    def test_terminate_all(self):
        """测试终止所有命令"""
        # 创建多个mock命令
        commands = [Mock(spec=Command) for _ in range(3)]
        for cmd in commands:
            self.manager.register(cmd)

        # 测试正常终止
        self.manager.terminate_all()
        for cmd in commands:
            cmd.terminate.assert_called_once()
        self.assertEqual(len(self.manager._active_commands), 0)

    def test_terminate_all_with_errors(self):
        """测试终止命令时出现错误的情况"""
        # 创建一个正常命令和一个会抛出异常的命令
        normal_cmd = Mock(spec=Command)
        error_cmd = Mock(spec=Command)
        error_cmd.terminate.side_effect = Exception("Mock error")

        self.manager.register(normal_cmd)
        self.manager.register(error_cmd)

        # 即使有命令终止失败，也应该继续终止其他命令
        with patch("loguru.logger.error") as mock_logger:
            self.manager.terminate_all()

            # 验证日志记录
            mock_logger.assert_called_once()
            self.assertIn("Mock error", mock_logger.call_args[0][0])

        # 验证所有命令都被尝试终止
        normal_cmd.terminate.assert_called_once()
        error_cmd.terminate.assert_called_once()

        # 验证所有命令都被取消注册
        self.assertEqual(len(self.manager._active_commands), 0)


if __name__ == "__main__":
    unittest.main()
