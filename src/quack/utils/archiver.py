#!/usr/bin/env python3

from __future__ import annotations

import subprocess
from collections.abc import Iterable


class Archiver:
    @staticmethod
    def archive(paths: Iterable[str], archive_path: str) -> None:
        paths_str = " ".join(paths) if paths else "-T /dev/null"
        cmd = f"tar czf {archive_path} {paths_str}"
        _ = subprocess.run(cmd, shell=True, check=True)

    @staticmethod
    def extract(archive_path: str, dest_path: str = ".") -> None:
        cmd = f"tar xf {archive_path} -C {dest_path}"
        _ = subprocess.run(cmd, shell=True, check=True)
