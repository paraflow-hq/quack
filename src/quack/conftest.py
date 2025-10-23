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
        "cwd": str(BASE_DIR.resolve()),
        "path": str((BASE_DIR / "quack.yaml").resolve()),
        "targets": [
            {
                "name": "quack:test",
                "description": "my test target",
                "dependencies": [
                    {
                        "type": "global",
                        "name": "command:quack",
                    }
                ],
                "outputs": {"paths": ["/tmp/quack-output"]},
                "operations": {"build": {"command": "echo hello > /tmp/quack-output"}},
            },
            {
                "name": "quack:test:child",
                "description": "继承输出",
                "dependencies": [{"type": "target", "name": "quack:test"}],
                "outputs": {"paths": ["/tmp/child-output"], "inherit": True},
                "operations": {"build": {"command": "echo child > /tmp/child-output"}},
            },
            {
                "name": "quack:test:child:no-inheritance",
                "description": "不继承输出",
                "dependencies": [{"type": "target", "name": "quack:test"}],
                "outputs": {"paths": ["/tmp/child-no-inheritance-output"]},
                "operations": {
                    "build": {
                        "command": "echo child-no-inheritance > /tmp/child-no-inheritance-output"
                    },
                },
            },
        ],
        "scripts": [
            {
                "name": "test",
                "description": "my test script",
                "command": {
                    "command": "echo test",
                    "base_path": str(BASE_DIR.resolve()),
                    "path": "/tmp",
                    "variables": {"TEST": "value"},
                },
            },
        ],
        "global_dependencies": [
            {
                "name": "source:quack",
                "type": "source",
                "paths": [
                    "^quack\\.yaml$",
                ],
                "propagate": True,
            },
            {
                "name": "command:quack",
                "type": "command",
                "commands": ["echo -n 1", "echo -n 2", "echo -n 3"],
            },
        ],
    }
    spec = Spec.model_validate(test_spec)
    spec.post_process()

    return spec
