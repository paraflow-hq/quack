#!/usr/bin/env python3

from __future__ import annotations

import os
from enum import Enum, StrEnum
from pathlib import Path
from typing import ClassVar, Literal, Self, override
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator
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


class CacheAccess(StrEnum):
    NONE = "none"
    READ = "read"
    READ_WRITE = "read_write"


class CacheLimits(BaseModel):
    max_archive_bytes: int = Field(default=2 * 1024**3, gt=0)
    max_expanded_bytes: int = Field(default=8 * 1024**3, gt=0)
    max_file_bytes: int = Field(default=2 * 1024**3, gt=0)
    max_entries: int = Field(default=100_000, gt=0)


class CloudConfig(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    prefix: str = "oss://yfd-wukong-test/app"
    region: str = ""
    endpoint: str = ""
    access_key_id: str = Field(default="", repr=False)
    access_key_secret: str = Field(default="", repr=False)
    session_token: str = Field(default="", repr=False)
    provider: Literal["auto", "r2"] = "auto"

    @model_validator(mode="after")
    def validate_credentials(self) -> Self:
        if bool(self.access_key_id) != bool(self.access_key_secret):
            raise ValueError("Cache credentials require both access key ID and secret")
        if self.session_token and not self.access_key_id:
            raise ValueError("A cache session token requires explicit access credentials")
        if self.provider == "r2":
            if not self.access_key_id:
                raise ValueError("R2 cache requires explicit cache-scoped credentials")
            endpoint = urlsplit(self.endpoint)
            if (
                endpoint.scheme != "https"
                or not endpoint.hostname
                or not endpoint.hostname.endswith(".r2.cloudflarestorage.com")
                or endpoint.username
                or endpoint.password
                or endpoint.query
                or endpoint.fragment
                or endpoint.path not in ("", "/")
                or not self.prefix.startswith("s3://")
            ):
                raise ValueError("R2 cache requires an explicit HTTPS R2 endpoint and s3:// prefix")
        return self


class Config(BaseSettings):
    cache: str = "dev"
    log_level: LogLevel = LogLevel.INFO
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    save_for_load: bool = False
    cache_access: CacheAccess = CacheAccess.READ_WRITE
    cache_namespace: str = "default"
    cache_limits: CacheLimits = Field(default_factory=CacheLimits)

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        hide_input_in_errors=True,
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
        explicit_file = os.environ.get("QUACK_CONFIG_FILE")
        if explicit_file and not Path(explicit_file).is_file():
            raise ValueError("The explicit Quack configuration file is missing")
        yaml_source = (
            YamlConfigSettingsSource(settings_cls, yaml_file=Path(explicit_file))
            if explicit_file
            else YamlConfigSettingsSource(settings_cls)
        )
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_source,
        )

    def setup_runtime(self):
        """为子进程设置环境变量"""
        os.environ["QUACK_CACHE"] = self.cache
        os.environ["QUACK_CACHE_ACCESS"] = self.cache_access.value
        os.environ["QUACK_CACHE_NAMESPACE"] = self.cache_namespace


if __name__ == "__main__":
    config = Config()
    print(config.model_dump(exclude={"cloud": {"access_key_id", "access_key_secret", "session_token"}}))
