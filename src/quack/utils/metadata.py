#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

from quack.exceptions import CacheSecurityError
from quack.utils.checksummer import generate_sha256sum


class Metadata(BaseModel):
    format_version: Literal[2] = 2
    target_name: str
    target_checksum: str = Field(pattern=r"^[a-f0-9]{64}$")
    file_checksum: str = Field(pattern=r"^[a-f0-9]{64}$")
    commit_sha: str = Field(pattern=r"^(?:[a-f0-9]{40}|)$")

    @classmethod
    def parse(cls, text: str) -> Metadata:
        try:
            data = json.loads(text)
            if not isinstance(data, dict) or data.get("format_version") != 2:
                raise ValueError("Unsupported cache format")
            return cls.model_validate(data)
        except (ValueError, ValidationError):
            raise CacheSecurityError("Invalid cache metadata") from None

    @classmethod
    def read(cls, path: str) -> Metadata:
        if Path(path).stat().st_size > 1024**2:
            raise CacheSecurityError("Cache metadata exceeds its size limit")
        return cls.parse(Path(path).read_text())

    @classmethod
    def generate(cls, path: str, output_path: str, target_checksum: str, commit_sha: str, *, target_name: str) -> None:
        data = cls(
            target_name=target_name,
            target_checksum=target_checksum,
            file_checksum=generate_sha256sum(path),
            commit_sha=commit_sha,
        )
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=Path(output_path).parent, delete=False) as output:
                temporary_path = output.name
                _ = output.write(data.model_dump_json())
            os.replace(temporary_path, output_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                os.unlink(temporary_path)
