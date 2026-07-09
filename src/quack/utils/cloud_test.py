from pathlib import Path
from unittest import mock

import pytest
from boto3.exceptions import RetriesExceededError, S3UploadFailedError
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from quack.exceptions import CloudStorageError, CloudStorageTransientError
from quack.utils.cloud import CloudClient


def _cloud_client_with_upload_error(error: Exception) -> CloudClient:
    client = CloudClient.__new__(CloudClient)
    client._base_path = ""
    client._bucket_name = "bucket"
    client._client = mock.Mock()
    client._client.upload_file.side_effect = error
    return client


def _cloud_client_with_download_error(error: Exception) -> CloudClient:
    client = CloudClient.__new__(CloudClient)
    client._base_path = ""
    client._bucket_name = "bucket"
    client._client = mock.Mock()
    client._client.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": "cache.tar.zst"}]}]
    client._client.download_file.side_effect = error
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


def _s3_upload_error_from_message(code: str) -> S3UploadFailedError:
    return S3UploadFailedError(
        f"Failed to upload cache.tar.zst: An error occurred ({code}) when calling the PutObject operation: {code}"
    )


def test_upload_wraps_incomplete_body_as_transient_error(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    local_file.write_bytes(b"cache")

    client = _cloud_client_with_upload_error(_s3_upload_error_from_client_error("IncompleteBody"))

    with pytest.raises(CloudStorageTransientError) as exc_info:
        client.upload(str(local_file), "dest/cache.tar.zst")

    assert exc_info.value.code == "IncompleteBody"


def test_upload_extracts_error_code_from_s3_transfer_message(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    local_file.write_bytes(b"cache")

    client = _cloud_client_with_upload_error(_s3_upload_error_from_message("IncompleteBody"))

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


def test_download_wraps_retries_exceeded_timeout_as_transient_error(tmp_path: Path):
    local_file = tmp_path / "cache.tar.zst"
    retry_error = RetriesExceededError(ReadTimeoutError(endpoint_url="https://s3.example"))
    client = _cloud_client_with_download_error(retry_error)

    with pytest.raises(CloudStorageTransientError) as exc_info:
        client.download("cache.tar.zst", str(local_file))

    assert exc_info.value.code is None


def test_exists_wraps_endpoint_connection_error_as_transient_error():
    client = CloudClient.__new__(CloudClient)
    client._base_path = ""
    client._bucket_name = "bucket"
    client._client = mock.Mock()
    client._client.head_object.side_effect = EndpointConnectionError(endpoint_url="https://s3.example")

    with pytest.raises(CloudStorageTransientError) as exc_info:
        client.exists("cache-metadata.json")

    assert exc_info.value.code is None
