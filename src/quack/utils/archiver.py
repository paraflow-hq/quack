#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import os
import shutil
import tarfile
import tempfile
from collections.abc import Iterable
from pathlib import Path

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

            # 同步到目标目录，基于内容比较，相同内容的文件不会被覆盖
            Archiver._sync_with_checksum(temp_dir, dest_path)

    @staticmethod
    def _sync_with_checksum(src_dir: str, dest_dir: str) -> None:
        """基于内容比较同步文件，内容相同时保持目标文件的时间戳"""
        src_path = Path(src_dir)
        dest_path = Path(dest_dir)

        for src_file in src_path.rglob("*"):
            if src_file.is_file():
                rel_path = src_file.relative_to(src_path)
                dest_file = dest_path / rel_path

                dest_file.parent.mkdir(parents=True, exist_ok=True)

                should_copy = True
                if dest_file.exists():
                    with open(src_file, "rb") as f1, open(dest_file, "rb") as f2:
                        src_hash = hashlib.sha256(f1.read()).hexdigest()
                        dest_hash = hashlib.sha256(f2.read()).hexdigest()
                        should_copy = src_hash != dest_hash

                if should_copy:
                    shutil.copy2(src_file, dest_file)
                    # 更新时间戳为当前时间
                    os.utime(dest_file, None)
