#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import tarfile
import tempfile
from collections.abc import Iterable

import zstandard as zstd


class Archiver:
    @staticmethod
    def archive(paths: Iterable[str], archive_path: str) -> None:
        with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp_tar:
            tmp_tar_path = tmp_tar.name

        try:
            with tarfile.open(tmp_tar_path, "w") as tar:
                for path in paths:
                    tar.add(path, arcname=path)

            with open(tmp_tar_path, "rb") as f_in:
                tar_data = f_in.read()

            cctx = zstd.ZstdCompressor()
            compressed_data = cctx.compress(tar_data)

            if dirname := os.path.dirname(archive_path):
                os.makedirs(dirname, exist_ok=True)
            with open(archive_path, "wb") as f_out:
                f_out.write(compressed_data)
        finally:
            if os.path.exists(tmp_tar_path):
                os.unlink(tmp_tar_path)

    @staticmethod
    def extract(archive_path: str, dest_path: str = ".") -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(archive_path, "rb") as f_in:
                compressed_data = f_in.read()

            dctx = zstd.ZstdDecompressor()
            tar_data = dctx.decompress(compressed_data)

            with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp_tar:
                tmp_tar.write(tar_data)
                tmp_tar_path = tmp_tar.name

            try:
                with tarfile.open(tmp_tar_path, "r") as tar:
                    tar.extractall(temp_dir, filter="data")
            finally:
                os.unlink(tmp_tar_path)

            # 使用 rsync 同步到目标目录，相同内容的文件不会被覆盖
            cmd_rsync = f"rsync --recursive --links --checksum {temp_dir}/ {dest_path}/"
            _ = subprocess.run(cmd_rsync, shell=True, check=True)
