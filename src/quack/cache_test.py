import os
from unittest import mock

from quack.cache import TargetCacheBackendTypeOSS
from quack.config import Config


class TestTargetCacheBackendTypeOSS:
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("quack.cache.OSSClient")
    def test_load_exists(
        self,
        mock_oss_client_class: mock.Mock,
        mock_local_backend: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        # Mock OSSClient 实例
        mock_oss_client = mock.Mock()
        mock_oss_client_class.return_value = mock_oss_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = True
        backend.load(target)
        mock_local_backend.return_value.load.assert_called_once()
        # 验证 update_access_time 被调用（上传 metadata）
        assert mock_oss_client.upload.called

    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("quack.cache.OSSClient")
    def test_load_not_exists(
        self,
        mock_oss_client_class: mock.Mock,
        mock_local_backend: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        # Mock OSSClient 实例
        mock_oss_client = mock.Mock()
        mock_oss_client_class.return_value = mock_oss_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, mock_test_spec.app_name)

        mock_local_backend.return_value.exists.return_value = False
        backend.load(target)
        # 验证从 OSS 下载了归档和元数据
        assert mock_oss_client.download.call_count == 2
        # 验证本地加载被调用
        assert mock_local_backend.return_value.load.call_count == 1

    @mock.patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}, clear=True)
    @mock.patch("quack.cache.TargetCacheBackendTypeLocal")
    @mock.patch("quack.cache.OSSClient")
    def test_save(
        self,
        mock_oss_client_class: mock.Mock,
        mock_local_backend: mock.Mock,
        mock_test_spec: mock.Mock,
    ):
        # Mock OSSClient 实例
        mock_oss_client = mock.Mock()
        mock_oss_client_class.return_value = mock_oss_client

        config = Config.model_construct()
        target = mock_test_spec.targets["quack:test"]
        target._checksum_value = ""
        backend = TargetCacheBackendTypeOSS(config, mock_test_spec.app_name)

        backend.save(target)
        # 验证本地保存被调用
        assert mock_local_backend.return_value.save.call_count == 1
        # 验证上传归档和元数据到 OSS
        assert mock_oss_client.upload.call_count == 2
