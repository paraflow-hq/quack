import os
from unittest import mock

from quack.cache import TargetCacheBackendTypeOSS
from quack.config import Config
from quack.spec import Spec


class TestTargetCacheBackendTypeOSS:
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("subprocess.run")
    def test_load_exists(self, mock_run: mock.Mock, mock_local_backend: mock.Mock):
        config = Config.model_construct()
        spec = Spec.get()
        target = spec.targets["quack"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, spec.app_name)

        mock_local_backend.return_value.exists.return_value = True
        backend.load(target)
        mock_local_backend.return_value.load.assert_called_once()
        mock_run.assert_called_once()

    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("subprocess.run")
    def test_load_not_exists(self, mock_run: mock.Mock, mock_local_backend: mock.Mock):
        config = Config.model_construct()
        spec = Spec.get()
        target = spec.targets["quack"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, spec.app_name)

        mock_local_backend.return_value.exists.return_value = False
        backend.load(target)
        assert mock_local_backend.return_value.load.call_count == 1
        assert mock_run.call_count == 3

    @mock.patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}, clear=True)
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("subprocess.run")
    def test_save(self, mock_run: mock.Mock, mock_local_backend: mock.Mock):
        config = Config.model_construct()
        spec = Spec.get()
        target = spec.targets["quack"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, spec.app_name)

        backend.save(target)
        assert mock_local_backend.return_value.save.call_count == 1
        assert mock_run.call_count == 2

    @mock.patch.dict(
        os.environ,
        {
            "PATH": "/usr/bin:/bin",
            "GITHUB_SHA": "391562ccc2e3f99ea834d2c0a6bc7bc7799c0312",
        },
        clear=True,
    )
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("subprocess.run")
    def test_save_for_load(self, mock_run: mock.Mock, mock_local_backend: mock.Mock):
        config = Config.model_construct()
        config.save_for_load = True
        spec = Spec.get()
        target = spec.targets["quack"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, spec.app_name)

        backend.save(target)
        assert mock_local_backend.return_value.save.call_count == 1
        assert mock_run.call_count == 3
