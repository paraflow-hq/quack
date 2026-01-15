#!/usr/bin/env python3

from __future__ import annotations

from loguru import logger

from quack.models.command import Command


class CommandManager:
    """命令管理器，用于跟踪和管理所有活跃的命令"""

    _active_commands: list[Command]
    _instance: CommandManager | None = None

    def __init__(self) -> None:
        if CommandManager._instance is not None:
            raise RuntimeError("CommandManager 是单例类，请使用 get() 方法获取实例")
        self._active_commands = []
        CommandManager._instance = self

    @classmethod
    def get(cls) -> CommandManager:
        if cls._instance is None:
            cls._instance = CommandManager()
        return cls._instance

    def register(self, command: Command) -> None:
        """注册一个活跃的命令"""
        self._active_commands.append(command)

    def unregister(self, command: Command) -> None:
        """取消注册一个命令"""
        if command in self._active_commands:
            self._active_commands.remove(command)

    def terminate_all(self) -> None:
        """终止所有活跃的命令"""
        for cmd in self._active_commands[:]:  # 创建副本避免在迭代时修改
            try:
                cmd.terminate()
            except Exception as e:
                logger.error(f"终止命令时出错: {e}")
            finally:
                self.unregister(cmd)
