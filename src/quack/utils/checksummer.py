#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import subprocess

from loguru import logger

from quack.exceptions import ChecksumError


class Checksummer:
    @staticmethod
    def generate(path: str, output_path: str) -> None:
        cmd = f"sha256sum {path} > {output_path}"
        _ = subprocess.run(cmd, shell=True, check=True)

    @staticmethod
    def verify(path: str, checksum_path: str) -> None:
        logger.info("正在校验 Checksum 值...")
        cmd = f"sha256sum -c {checksum_path} > /dev/null"
        try:
            _ = subprocess.run(cmd, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            raise ChecksumError(f"校验 Checksum 失败: {path}") from e


def generate_sha256sum(path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):  # 按块读取
            sha256_hash.update(block)
    return sha256_hash.hexdigest()
