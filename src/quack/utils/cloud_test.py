from pathlib import Path
from unittest import mock

import pytest
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from quack.exceptions import CloudStorageError, CloudStorageTransientError
from quack.utils.cloud import CloudClient


def test_upload_wraps_incomplete_body_as_transient_error(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    local_file.write_bytes(b"cache")

    client = CloudClient.__new__(CloudClient)
    client._base_path = ""
    client._bucket_name = "bucket"
    client._client = mock.Mock()
    client._client.upload_file.side_effect = S3UploadFailedError(
        "Failed to upload cache.tar.zst: An error occurred (IncompleteBody) when calling the PutObject operation"
    )

    with pytest.raises(CloudStorageTransientError) as exc_info:
        client.upload(str(local_file), "dest/cache.tar.zst")

    assert exc_info.value.code == "IncompleteBody"


def test_upload_keeps_access_denied_as_non_transient_error(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    local_file.write_bytes(b"cache")

    client = CloudClient.__new__(CloudClient)
    client._base_path = ""
    client._bucket_name = "bucket"
    client._client = mock.Mock()
    client._client.upload_file.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "PutObject",
    )

    with pytest.raises(CloudStorageError) as exc_info:
        client.upload(str(local_file), "dest/cache.tar.zst")

    assert not isinstance(exc_info.value, CloudStorageTransientError)
    assert exc_info.value.code == "AccessDenied"
