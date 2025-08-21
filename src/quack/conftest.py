import os
from pathlib import Path

import pytest

from quack.spec import Spec

SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_DIR = SCRIPT_DIR.parent.parent

os.chdir(BASE_DIR)


@pytest.fixture(autouse=True)
def mock_test_spec():
    test_spec = {
        "app_name": "quack_test",
        "targets": [
            {
                "name": "quack",
                "description": "my test target",
                "dependencies": [],
                "outputs": {"paths": ["/tmp/quack-output"]},
                "operations": {"build": {"command": "echo hello > /tmp/quack-output"}},
            }
        ],
        "scripts": [
            {
                "name": "test",
                "description": "my test script",
                "command": {
                    "command": "echo test",
                    "path": "/tmp",
                    "variables": {"TEST": "value"},
                },
            }
        ],
        "global_dependencies": [
            {
                "name": "source:quack",
                "type": "source",
                "paths": [
                    "^quack\\.yaml$",
                    "^.*/quack\\.yaml$",
                    "^scripts/quack\\.py$",
                    "^scripts/quack/.*$",
                ],
            },
            {
                "name": "command:quack",
                "type": "command",
                "commands": ["echo -n 1", "echo -n 2", "echo -n 3"],
            },
        ],
    }
    return Spec(BASE_DIR, BASE_DIR / "quack.yaml", test_spec)
