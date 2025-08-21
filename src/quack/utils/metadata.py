#!/usr/bin/env python3

from __future__ import annotations

import json
import socket
from datetime import datetime

from quack.utils.checksummer import generate_sha256sum


class Metadata:
    @staticmethod
    def generate(path: str, output_path: str) -> None:
        with open(output_path, "w") as f:
            data = {
                "checksum": generate_sha256sum(path),
                "hostname": socket.gethostname(),
                "created_at": datetime.now().isoformat(),
            }
            json.dump(data, f)
