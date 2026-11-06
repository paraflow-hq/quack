"""@no-uc: Technical regression; the real CLI must preserve cache trust and filesystem boundaries."""

import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import zstandard

SOURCE = os.environ.get("QUACK_TEST_SOURCE", str(Path(__file__).resolve().parents[1]))
SHA = "a" * 40
CHECKSUM = "b" * 64
ROOT = ".quack-cache-v2/demo"


def archive(files: dict[str, bytes]) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as tar:
        for name, data in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            tar.addfile(entry, io.BytesIO(data))
    return zstandard.ZstdCompressor().compress(stream.getvalue())


class Store:
    def __init__(self, *, writable: bool):
        self.objects: dict[str, bytes] = {}
        self.writable = writable
        self.requests: list[tuple[str, str, str | None]] = []
        self.list_requests = 0
        self.archive_status = 200

    def seed(self, data: bytes, *, digest: str | None = None, checksum: str = CHECKSUM):
        metadata = json.dumps(
            {
                "format_version": 2,
                "target_name": "demo:build",
                "target_checksum": checksum,
                "file_checksum": digest or hashlib.sha256(data).hexdigest(),
                "commit_sha": SHA,
            }
        ).encode()
        key = f"{ROOT}/demo:build/{checksum[:2]}/{checksum[2:]}"
        self.objects[f"{key}/demo:build.tar.zst"] = data
        self.objects[f"{key}/_metadata.json"] = metadata
        self.objects[f"{ROOT}/_commits/{SHA}/demo:build.json"] = metadata
        # The same fixture can reproduce the pre-v2 implementation's failures.
        for name, value in list(self.objects.items()):
            self.objects[name.replace(".quack-cache-v2/", ".quack-cache/")] = value


@contextmanager
def s3_server(*, writable: bool = False):
    store = Store(writable=writable)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object):
            pass

        def do_HEAD(self):
            self.respond(False)

        def do_GET(self):
            self.respond(True)

        def do_PUT(self):
            key = unquote(urlsplit(self.path).path.removeprefix("/bucket/"))
            store.requests.append(("PUT", key, self.headers.get("X-Amz-Security-Token")))
            if store.writable:
                store.objects[key] = self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("ETag", '"fixture"')
                self.end_headers()
            else:
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b"<Error><Code>AccessDenied</Code></Error>")

        def respond(self, body: bool):
            parsed = urlsplit(self.path)
            key = unquote(parsed.path.removeprefix("/bucket/"))
            store.requests.append((self.command, key, self.headers.get("X-Amz-Security-Token")))
            if parsed.query:
                store.list_requests += 1
                prefix = parse_qs(parsed.query).get("prefix", [""])[0]
                keys = [key for key in store.objects if key.startswith(prefix)]
                data = (
                    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    "<IsTruncated>false</IsTruncated>"
                    + "".join(
                        f"<Contents><Key>{key}</Key><Size>{len(store.objects[key])}</Size></Contents>" for key in keys
                    )
                    + "</ListBucketResult>"
                ).encode()
                status = 200
            else:
                data = store.objects.get(key, b"<Error><Code>NoSuchKey</Code></Error>")
                status = 200 if key in store.objects else 404
                if self.command == "GET" and key.endswith(".tar.zst") and store.archive_status != 200:
                    status = store.archive_status
                    code = "AccessDenied" if status == 403 else "ServiceUnavailable"
                    data = f"<Error><Code>{code}</Code></Error>".encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("ETag", '"fixture"')
            self.end_headers()
            if body:
                self.wfile.write(data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield store, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def run_cli(root: Path, *args: str, endpoint: str = "", settings: dict[str, str] | None = None):
    root.mkdir(exist_ok=True)
    (root / "quack.yaml").write_text("""app_name: demo
targets:
  - name: demo:build
    description: Build a fixture
    outputs:
      paths: [dist]
    operations:
      build:
        command: mkdir -p dist && printf built > dist/result.txt && printf ran >> builds.txt
""")
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(root / "home"),
        "PYTHONPATH": SOURCE,
        "XDG_CONFIG_HOME": str(root / "config"),
        "XDG_CACHE_HOME": str(root / "cache"),
        "CI": "true",
        "GITHUB_SHA": SHA,
        "QUACK_CACHE": "cloud",
        "QUACK_CLOUD__PREFIX": "s3://bucket",
        "QUACK_CLOUD__REGION": "auto",
        "QUACK_CLOUD__ENDPOINT": endpoint,
        "QUACK_CLOUD__ACCESS_KEY_ID": "fixture-id",
        "QUACK_CLOUD__ACCESS_KEY_SECRET": "fixture-secret",
        "QUACK_SAVE_FOR_LOAD": "true",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ENDPOINT_URL_S3": endpoint or "http://127.0.0.1:9",
        "NO_PROXY": "127.0.0.1,localhost",
    }
    env.update(settings or {})
    return subprocess.run(
        [sys.executable, "-m", "quack", *args], cwd=root, env=env, capture_output=True, text=True, timeout=20
    )


def test_security_read_miss_builds_without_cloud_writes(tmp_path):
    with s3_server() as (store, endpoint):
        result = run_cli(tmp_path, "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"})
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "dist/result.txt").read_text() == "built"
    assert not any(method == "PUT" for method, _, _ in store.requests)


def test_security_read_load_only_uses_exact_objects_and_never_writes(tmp_path):
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"cached"}))
        key = f"{ROOT}/demo:build/{CHECKSUM[:2]}/{CHECKSUM[2:]}/demo:build.tar.zst"
        store.objects[f"{key}.extra/../../../outside.txt"] = b"poison"
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "dist/result.txt").read_text() == "cached"
    assert not (tmp_path / "builds.txt").exists()
    assert not any(method == "PUT" for method, _, _ in store.requests)
    assert store.list_requests == 0


def test_security_none_never_contacts_cloud(tmp_path):
    with s3_server() as (store, endpoint):
        result = run_cli(tmp_path, "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "none"})
    assert result.returncode == 0, result.stderr
    assert store.requests == []


def test_security_none_rejects_load_only_before_any_cloud_request(tmp_path):
    with s3_server() as (store, endpoint):
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "none"}
        )
    assert result.returncode != 0
    assert store.requests == []
    assert not (tmp_path / "builds.txt").exists()


@pytest.mark.parametrize(
    "payload",
    [
        {"dist/result.txt": b"cached", "scripts/deploy.sh": b"poison"},
        {"dist/result.txt": b"cached", "../outside.txt": b"poison"},
    ],
)
def test_security_rejects_archive_outside_declared_outputs_before_install(tmp_path, payload):
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist/result.txt").write_text("original")
    with s3_server() as (store, endpoint):
        store.seed(archive(payload))
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode != 0
    assert (tmp_path / "dist/result.txt").read_text() == "original"
    assert not (tmp_path / "scripts/deploy.sh").exists()
    assert not (tmp_path / "builds.txt").exists()


def test_security_rejects_existing_destination_symlink(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "result.txt").write_text("original")
    (tmp_path / "dist").symlink_to(outside, target_is_directory=True)
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"poison"}))
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode != 0
    assert (outside / "result.txt").read_text() == "original"


def test_security_rejects_modified_archive_digest(tmp_path):
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"poison"}), digest="0" * 64)
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode != 0
    assert not (tmp_path / "dist/result.txt").exists()


def test_security_rejects_invalid_index_without_following_its_checksum(tmp_path):
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"poison"}), checksum="../escape")
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode != 0
    assert len(store.requests) == 1


def test_security_session_token_reaches_s3_requests(tmp_path):
    with s3_server() as (store, endpoint):
        result = run_cli(
            tmp_path,
            "demo:build",
            endpoint=endpoint,
            settings={"QUACK_CACHE_ACCESS": "read", "QUACK_CLOUD__SESSION_TOKEN": "fixture-session"},
        )
    assert result.returncode == 0, result.stderr
    assert store.requests and all(token == "fixture-session" for _, _, token in store.requests)


def test_security_storage_change_does_not_reuse_local_cache(tmp_path):
    settings = {"QUACK_CACHE": "local", "QUACK_SAVE_FOR_LOAD": "false"}
    first = run_cli(tmp_path, "demo:build", settings={**settings, "QUACK_CLOUD__PREFIX": "s3://first"})
    assert first.returncode == 0, first.stderr
    (tmp_path / "dist/result.txt").write_text("changed")
    second = run_cli(tmp_path, "demo:build", settings={**settings, "QUACK_CLOUD__PREFIX": "s3://second"})
    assert second.returncode == 0, second.stderr
    assert (tmp_path / "builds.txt").read_text() == "ranran"


@pytest.mark.parametrize("limit", ["MAX_FILE_BYTES", "MAX_ARCHIVE_BYTES", "MAX_EXPANDED_BYTES", "MAX_ENTRIES"])
def test_security_limits_reject_archive_before_install(tmp_path, limit):
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"x" * 1024, "dist/other.txt": b"other"}))
        result = run_cli(
            tmp_path,
            "--load-only",
            "demo:build",
            endpoint=endpoint,
            settings={
                "QUACK_CACHE_ACCESS": "read",
                f"QUACK_CACHE_LIMITS__{limit}": "1" if limit == "MAX_ENTRIES" else "32",
            },
        )
    assert result.returncode != 0
    assert not (tmp_path / "dist/result.txt").exists()


@pytest.mark.parametrize("cache", ["local", "false"])
def test_security_local_or_disabled_cache_never_publishes_a_commit_index(tmp_path, cache):
    with s3_server() as (store, endpoint):
        result = run_cli(tmp_path, "demo:build", endpoint=endpoint, settings={"QUACK_CACHE": cache})
    assert result.returncode == 0, result.stderr
    assert store.requests == []


def test_security_writer_and_cold_read_only_consumer_share_verified_v2_artifact(tmp_path):
    with s3_server(writable=True) as (store, endpoint):
        writer = run_cli(tmp_path, "demo:build", endpoint=endpoint)
        assert writer.returncode == 0, writer.stderr
        assert json.loads(store.objects[f"{ROOT}/_commits/{SHA}/demo:build.json"])["format_version"] == 2
        shutil.rmtree(tmp_path / "cache")
        (tmp_path / "dist/result.txt").unlink()
        store.requests.clear()
        reader = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
        assert reader.returncode == 0, reader.stderr
        assert not any(method == "PUT" for method, _, _ in store.requests)
    assert (tmp_path / "dist/result.txt").read_text() == "built"
    assert (tmp_path / "builds.txt").read_text() == "ran"


def test_security_r2_rejects_missing_endpoint_without_network_access(tmp_path):
    result = run_cli(tmp_path, "demo:build", settings={"QUACK_CLOUD__PROVIDER": "r2"})
    assert result.returncode != 0
    assert "explicit HTTPS R2 endpoint" in result.stderr
    assert "fixture-secret" not in result.stderr
    assert not (tmp_path / "builds.txt").exists()


def test_security_regular_output_replacement_does_not_modify_a_hard_link(tmp_path):
    outside = tmp_path / "outside.txt"
    outside.write_text("original")
    (tmp_path / "dist").mkdir()
    os.link(outside, tmp_path / "dist/result.txt")
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"cached"}))
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "dist/result.txt").read_text() == "cached"
    assert outside.read_text() == "original"


def test_security_rejects_archive_link_before_installing_valid_files(tmp_path):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as tar:
        entry = tarfile.TarInfo("dist/result.txt")
        entry.size = 6
        tar.addfile(entry, io.BytesIO(b"cached"))
        link = tarfile.TarInfo("dist/link")
        link.type = tarfile.SYMTYPE
        link.linkname = "../outside.txt"
        tar.addfile(link)
    with s3_server() as (store, endpoint):
        store.seed(zstandard.ZstdCompressor().compress(stream.getvalue()))
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode != 0
    assert not (tmp_path / "dist/result.txt").exists()


def test_security_rejects_oversized_zstd_window_before_install(tmp_path):
    raw_tar = zstandard.ZstdDecompressor().decompress(archive({"dist/result.txt": b"cached"}))
    stream = io.BytesIO()
    with zstandard.ZstdCompressor().stream_writer(stream, closefd=False) as writer:
        writer.write(raw_tar)
    data = bytearray(stream.getvalue())
    data[5] = 0x90  # A streaming frame with a 256 MiB window descriptor.
    assert zstandard.get_frame_parameters(data).window_size == 256 * 1024**2
    with s3_server() as (store, endpoint):
        store.seed(bytes(data))
        result = run_cli(
            tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"}
        )
    assert result.returncode != 0
    assert not (tmp_path / "dist/result.txt").exists()


def test_security_explicit_config_file_ignores_stale_workspace_config(tmp_path):
    private_config = tmp_path.parent / f"{tmp_path.name}-private.yaml"
    private_config.write_text("cache_access: read\n")
    (tmp_path / ".quack.yaml").write_text("cache_access: none\n")
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"cached"}))
        result = run_cli(
            tmp_path,
            "--load-only",
            "demo:build",
            endpoint=endpoint,
            settings={"QUACK_CONFIG_FILE": str(private_config)},
        )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "dist/result.txt").read_text() == "cached"
    assert not any(method == "PUT" for method, _, _ in store.requests)


def test_security_missing_explicit_config_file_does_not_fall_back(tmp_path):
    with s3_server() as (store, endpoint):
        result = run_cli(
            tmp_path, "demo:build", endpoint=endpoint, settings={"QUACK_CONFIG_FILE": str(tmp_path / "missing.yaml")}
        )
    assert result.returncode != 0
    assert store.requests == []
    assert not (tmp_path / "builds.txt").exists()


def test_security_restore_preserves_identical_file_timestamp_and_replaces_modified_content(tmp_path):
    with s3_server() as (store, endpoint):
        store.seed(archive({"dist/result.txt": b"cached"}))
        settings = {"QUACK_CACHE_ACCESS": "read"}
        first = run_cli(tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings=settings)
        assert first.returncode == 0, first.stderr
        output = tmp_path / "dist/result.txt"
        historical_time = 1_600_000_000_000_000_000
        os.utime(output, ns=(historical_time, historical_time))
        same = run_cli(tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings=settings)
        assert same.returncode == 0, same.stderr
        assert output.stat().st_mtime_ns == historical_time
        output.write_text("modified")
        os.utime(output, ns=(historical_time, historical_time))
        replaced = run_cli(tmp_path, "--load-only", "demo:build", endpoint=endpoint, settings=settings)
        assert replaced.returncode == 0, replaced.stderr
        assert output.read_text() == "cached"
        assert output.stat().st_mtime_ns > historical_time


@pytest.mark.parametrize("status", [403, 503])
def test_security_remote_archive_error_respects_normal_fallback_boundary(tmp_path, status):
    with s3_server() as (store, endpoint):
        checksum = hashlib.sha256(repr([]).encode()).hexdigest()
        store.seed(archive({"dist/result.txt": b"cached"}), checksum=checksum)
        store.archive_status = status
        result = run_cli(tmp_path, "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"})
    if status == 403:
        assert result.returncode != 0
        assert not (tmp_path / "builds.txt").exists()
    else:
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "dist/result.txt").read_text() == "built"
    assert not any(method == "PUT" for method, _, _ in store.requests)


def test_security_corrupt_zstd_archive_can_rebuild_without_load_only(tmp_path):
    with s3_server() as (store, endpoint):
        checksum = hashlib.sha256(repr([]).encode()).hexdigest()
        store.seed(b"invalid-zstd-frame", checksum=checksum)
        result = run_cli(tmp_path, "demo:build", endpoint=endpoint, settings={"QUACK_CACHE_ACCESS": "read"})
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "dist/result.txt").read_text() == "built"
    assert not any(method == "PUT" for method, _, _ in store.requests)
