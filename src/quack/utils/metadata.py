#!/usr/bin/env python3

from __future__ import annotations

import json
import socket
from datetime import datetime

from quack.utils.checksummer import generate_sha256sum


class Metadata:
    @staticmethod
    def generate(path: str, output_path: str, target_checksum: str, commit_sha: str) -> None:
        with open(output_path, "w") as f:
            data = {
                "target_checksum": target_checksum,
                "file_checksum": generate_sha256sum(path),
                "hostname": socket.gethostname(),
                "commit_sha": commit_sha,
                "created_at": datetime.now().isoformat(),
            }
            json.dump(data, f)
