#!/usr/bin/env python3

from __future__ import annotations

from enum import Enum
from typing import ClassVar, override

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)
from xdg_base_dirs import xdg_config_home


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"


class DBConfig(BaseSettings):
    host: str = "localhost"
    port: int = 3306
    database: str = "quack"
    user: str = "quack"
    password: str = "quack"


class OSSConfig(BaseSettings):
    prefix: str = "oss://yfd-wukong-test/app"
    endpoint: str = ""
    access_key_id: str = ""
    access_key_secret: str = ""
    loglevel: str = "info"
    parallel_level: int = 50


class Config(BaseSettings):
    remote_host: str = ""
    remote_root: str = ""
    cache: str = "dev"
    log_level: LogLevel = LogLevel.INFO
    db: DBConfig = DBConfig()
    oss: OSSConfig = OSSConfig()

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_nested_delimiter="__",
        env_prefix="QUACK_",
        # TODO: 如果将 runtime 中的变量合并进来，此行可以去掉
        extra="ignore",
        yaml_file=[
            xdg_config_home() / "quack" / "config.yaml",
            ".quack.yaml",
        ],
        yaml_file_encoding="utf-8",
    )

    @override
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
        )


if __name__ == "__main__":
    config = Config()
    print(config.model_dump())
