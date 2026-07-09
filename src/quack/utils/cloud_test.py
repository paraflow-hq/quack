from pathlib import Path
from unittest import mock

import pytest
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from quack.exceptions import CloudStorageError, CloudStorageTransientError
from quack.utils.cloud import CloudClient


def _cloud_client_with_upload_error(error: Exception) -> CloudClient:
    client = CloudClient.__new__(CloudClient)
    client._base_path = ""
    client._bucket_name = "bucket"
    client._client = mock.Mock()
    client._client.upload_file.side_effect = error
    return client


def _s3_upload_error_from_client_error(code: str) -> S3UploadFailedError:
    client_error = ClientError(
        {"Error": {"Code": code, "Message": code}},
        "PutObject",
    )
    try:
        raise S3UploadFailedError("Failed to upload cache.tar.zst") from client_error
    except S3UploadFailedError as e:
        return e


def test_upload_wraps_incomplete_body_as_transient_error(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    local_file.write_bytes(b"cache")

    client = _cloud_client_with_upload_error(_s3_upload_error_from_client_error("IncompleteBody"))

    with pytest.raises(CloudStorageTransientError) as exc_info:
        client.upload(str(local_file), "dest/cache.tar.zst")

    assert exc_info.value.code == "IncompleteBody"


def test_upload_keeps_access_denied_as_non_transient_error(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    local_file.write_bytes(b"cache")

    client = _cloud_client_with_upload_error(_s3_upload_error_from_client_error("AccessDenied"))

    with pytest.raises(CloudStorageError) as exc_info:
        client.upload(str(local_file), "dest/cache.tar.zst")

    assert not isinstance(exc_info.value, CloudStorageTransientError)
    assert exc_info.value.code == "AccessDenied"
