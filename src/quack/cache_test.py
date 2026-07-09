import os
from unittest import mock

import pytest
import zstandard as zstd

from quack.cache import TargetCacheBackendTypeCloud
from quack.config import Config
from quack.exceptions import CloudStorageError, CloudStorageTransientError


class TestTargetCacheBackendTypeCloud:
    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_load_exists(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        # Mock 云存储客户端
        mock_cloud_client = mock.Mock()
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = True
        backend.load(target)
        mock_local_backend.return_value.load.assert_called_once()
        # 验证 update_access_time 被调用（上传 metadata）
        assert mock_cloud_client.upload.called

    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_load_exists_ignores_transient_access_time_update_failure(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        mock_cloud_client = mock.Mock()
        mock_cloud_client.upload.side_effect = CloudStorageTransientError(
            "上传文件失败",
            "IncompleteBody",
            code="IncompleteBody",
        )
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = True

        backend.load(target)

        mock_local_backend.return_value.load.assert_called_once()

    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_load_exists_raises_non_transient_access_time_update_failure(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        mock_cloud_client = mock.Mock()
        mock_cloud_client.upload.side_effect = CloudStorageError("上传文件失败", "AccessDenied", code="AccessDenied")
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = True

        with pytest.raises(CloudStorageError):
            backend.load(target)

    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_load_not_exists(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        # Mock 云存储客户端
        mock_cloud_client = mock.Mock()
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = False
        backend.load(target)
        # 验证从云存储下载了归档和元数据
        assert mock_cloud_client.download.call_count == 2
        # 验证本地加载被调用
        assert mock_local_backend.return_value.load.call_count == 1

    @mock.patch("quack.cache.shutil.rmtree")
    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_load_corrupt_local_cache_falls_back_to_cloud(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_rmtree: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        mock_cloud_client = mock.Mock()
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = True
        # First call (local) raises; second call (after re-download) succeeds
        mock_local_backend.return_value.load.side_effect = [zstd.ZstdError("did not decompress full frame"), None]

        backend.load(target)

        assert mock_cloud_client.download.call_count == 2
        assert mock_rmtree.called

    @mock.patch("quack.cache.shutil.rmtree")
    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_load_corrupt_cloud_cache_raises(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_rmtree: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        mock_cloud_client = mock.Mock()
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = False
        mock_local_backend.return_value.load.side_effect = zstd.ZstdError("did not decompress full frame")

        with pytest.raises(zstd.ZstdError):
            backend.load(target)

        assert mock_rmtree.called

    @mock.patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}, clear=True)
    @mock.patch("quack.cache.CloudClient")
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    def test_save(
        self,
        mock_local_backend: mock.Mock,
        mock_cloud_client_class: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        # Mock 云存储客户端
        mock_cloud_client = mock.Mock()
        mock_cloud_client_class.return_value = mock_cloud_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeCloud(config, mock_test_spec.app_name)

        backend.save(target)
        # 验证本地保存被调用
        assert mock_local_backend.return_value.save.call_count == 1
        # 验证上传归档和元数据到云存储
        assert mock_cloud_client.upload.call_count == 2
