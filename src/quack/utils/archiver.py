#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Iterable


class Archiver:
    @staticmethod
    def archive(paths: Iterable[str], archive_path: str) -> None:
        paths_str = " ".join(paths) if paths else "-T /dev/null"
        cmd = f"tar czf {archive_path} {paths_str}"
        env = os.environ.copy()
        # 防止 macOS tar 包含 ._ 资源分支文件
        env["COPYFILE_DISABLE"] = "1"
        _ = subprocess.run(cmd, shell=True, check=True, env=env)

    @staticmethod
    def extract(archive_path: str, dest_path: str = ".") -> None:
        # Create a temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            # 先解压到临时目录
            cmd_extract = f"tar xf {archive_path} -C {temp_dir}"
            _ = subprocess.run(cmd_extract, shell=True, check=True)

            # 使用 rsync 同步到目标目录，相同内容的文件不会被覆盖
            cmd_rsync = f"rsync --recursive --links --checksum {temp_dir}/ {dest_path}/"
            _ = subprocess.run(cmd_rsync, shell=True, check=True)
