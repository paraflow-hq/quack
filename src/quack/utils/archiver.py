#!/usr/bin/env python3

from __future__ import annotations

import fnmatch
import glob
import hashlib
import os
import re
import shutil
import stat
import tarfile
import tempfile
import uuid
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import IO

import zstandard as zstd

from quack.config import CacheLimits
from quack.exceptions import CacheSecurityError

PAX_RECORD = re.compile(rb"([1-9][0-9]{0,6}) ([a-z]+)=")
PAX_FIELDS = {b"path", b"uid", b"gid", b"uname", b"gname", b"mtime", b"size", b"hdrcharset"}


class Archiver:
    @staticmethod
    def archive(paths: Iterable[str], archive_path: str) -> None:
        with tempfile.TemporaryDirectory() as staging:
            tar_path = Path(staging) / "archive.tar"
            seen: set[str] = set()
            with tarfile.open(tar_path, "w", dereference=True) as tar:
                for pattern in sorted(paths):
                    matches = glob.glob(pattern)
                    if not matches:
                        raise FileNotFoundError(f"No output matches {pattern}")
                    for match in sorted(matches):
                        root = Path(match)
                        for path in [root, *sorted(root.rglob("*"))] if root.is_dir() else [root]:
                            if path.is_symlink() or not (path.is_file() or path.is_dir()):
                                raise CacheSecurityError("Cache outputs cannot contain links or special files")
                            name = path.as_posix().lstrip("/")
                            if name not in seen:
                                tar.add(path, arcname=name, recursive=False)
                                seen.add(name)
            Path(archive_path).parent.mkdir(parents=True, exist_ok=True)
            temporary_path = None
            try:
                with (
                    tar_path.open("rb") as source,
                    tempfile.NamedTemporaryFile(dir=Path(archive_path).parent, delete=False) as output,
                    zstd.ZstdCompressor().stream_writer(output, closefd=False) as compressor,
                ):
                    temporary_path = output.name
                    shutil.copyfileobj(source, compressor, length=1024**2)
                os.replace(temporary_path, archive_path)
                temporary_path = None
            finally:
                if temporary_path is not None:
                    os.unlink(temporary_path)

    @staticmethod
    def extract(
        archive_path: str,
        dest_path: str = ".",
        *,
        outputs: Iterable[str],
        expected_checksum: str,
        limits: CacheLimits | None = None,
    ) -> None:
        budget = limits or CacheLimits()
        destination = Path(dest_path).resolve()
        patterns = Archiver._output_patterns(outputs, destination)
        with tempfile.TemporaryDirectory() as staging:
            tar_path = Path(staging) / "archive.tar"
            with (
                os.fdopen(os.open(archive_path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source,
                tar_path.open("wb") as output,
            ):
                if os.fstat(source.fileno()).st_size > budget.max_archive_bytes:
                    raise CacheSecurityError("Cache archive exceeds its compressed size limit")
                if hashlib.file_digest(source, "sha256").hexdigest() != expected_checksum:
                    raise CacheSecurityError("Cache archive checksum mismatch")
                source.seek(0)
                with zstd.ZstdDecompressor(max_window_size=128 * 1024**2).stream_reader(source) as decompressor:
                    expanded = 0
                    while chunk := decompressor.read(1024**2):
                        expanded += len(chunk)
                        if expanded > budget.max_expanded_bytes:
                            raise CacheSecurityError("Cache archive exceeds its expanded size limit")
                        _ = output.write(chunk)
            Archiver._validate_headers(tar_path, budget)
            with tarfile.open(tar_path, "r:") as tar:
                entries: list[tarfile.TarInfo] = []
                seen: set[str] = set()
                total = 0
                for entry in tar:
                    name = Archiver._member_name(entry.name)
                    if name in seen:
                        raise CacheSecurityError("Duplicate cache archive member")
                    seen.add(name)
                    if entry.issparse() or not (entry.isfile() or entry.isdir()):
                        raise CacheSecurityError("Cache archive contains a link or special file")
                    if not Archiver._allowed(name, patterns, directory=entry.isdir()):
                        raise CacheSecurityError("Cache archive member is outside declared outputs")
                    total += entry.size
                    if (
                        entry.size > budget.max_file_bytes
                        or total > budget.max_expanded_bytes
                        or len(entries) >= budget.max_entries
                    ):
                        raise CacheSecurityError("Cache archive exceeds its file or entry limit")
                    Archiver._validate_destination(destination, name)
                    entries.append(entry)
                # Validate the entire archive and workspace before installing any member.
                root_fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    for entry in entries:
                        Archiver._install(tar, entry, root_fd)
                finally:
                    os.close(root_fd)

    @staticmethod
    def _member_name(name: str) -> str:
        parts = PurePosixPath(name).parts
        if not parts or name.startswith("/") or "\\" in name or ".." in parts or "\x00" in name:
            raise CacheSecurityError("Invalid cache archive path")
        return "/".join(parts)

    @staticmethod
    def _output_patterns(outputs: Iterable[str], destination: Path) -> list[tuple[str, ...]]:
        result = []
        for output in outputs:
            path = Path(output)
            if path.is_absolute():
                try:
                    path = path.relative_to(destination)
                except ValueError as error:
                    raise CacheSecurityError("Declared cache output is outside the workspace") from error
            name = Archiver._member_name(path.as_posix())
            result.append(PurePosixPath(name).parts)
        return result

    @staticmethod
    def _allowed(name: str, patterns: list[tuple[str, ...]], *, directory: bool) -> bool:
        parts = PurePosixPath(name).parts
        for pattern in patterns:
            common = min(len(pattern), len(parts))
            if (len(parts) >= len(pattern) or directory) and all(
                fnmatch.fnmatchcase(parts[i], pattern[i]) for i in range(common)
            ):
                return True
        return False

    @staticmethod
    def _validate_headers(path: Path, limits: CacheLimits) -> None:
        with path.open("rb") as stream:
            count = 0
            pending_extension = False
            pax_size = None
            while (block := stream.read(512)) and block != b"\0" * 512:
                try:
                    header = tarfile.TarInfo.frombuf(block, "utf-8", "surrogateescape")
                except tarfile.HeaderError as error:
                    raise CacheSecurityError("Invalid cache tar header") from error
                size = header.size
                if header.type in (tarfile.XHDTYPE, tarfile.GNUTYPE_LONGNAME):
                    if pending_extension or not 0 < size <= 1024**2:
                        raise CacheSecurityError("Cache tar extension exceeds its size or nesting limit")
                    pending_extension = True
                    if header.type == tarfile.XHDTYPE:
                        pax_size = Archiver._read_pax_size(stream.read(size))
                        _ = stream.seek(-size % 512, os.SEEK_CUR)
                    else:
                        _ = stream.seek(((size + 511) // 512) * 512, os.SEEK_CUR)
                    continue
                if header.type not in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.CONTTYPE, tarfile.DIRTYPE):
                    raise CacheSecurityError("Cache archive contains an unsupported tar member")
                # tarfile follows the effective size, including a local PAX override.
                size = pax_size if pax_size is not None else size
                count += 1
                if header.isdir() and size != 0:
                    raise CacheSecurityError("Cache tar directory has a nonzero size")
                pending_extension = False
                pax_size = None
                if count > limits.max_entries or not 0 <= size <= limits.max_file_bytes:
                    raise CacheSecurityError("Cache archive exceeds its header or file limit")
                _ = stream.seek(((size + 511) // 512) * 512, os.SEEK_CUR)
            if pending_extension or not block:
                raise CacheSecurityError("Cache archive has an incomplete tar member or terminator")

    @staticmethod
    def _read_pax_size(payload: bytes) -> int | None:
        # Only accept local PAX fields emitted by our writer. Sparse and global extensions
        # have their own layout rules and cannot be checked with ordinary member offsets.
        size = None
        offset = 0
        while offset < len(payload):
            record = PAX_RECORD.match(payload, offset)
            if record is None or record[2] not in PAX_FIELDS:
                raise CacheSecurityError("Unsupported cache tar PAX record")
            end = offset + int(record[1])
            if not record.end() < end <= len(payload) or payload[end - 1] != 10:
                raise CacheSecurityError("Invalid cache tar PAX record length")
            if record[2] == b"size":
                value = payload[record.end() : end - 1]
                if not value.isdigit() or len(value) > 20:
                    raise CacheSecurityError("Invalid cache tar PAX size")
                size = int(value)
            offset = end
        return size

    @staticmethod
    def _validate_destination(root: Path, name: str) -> None:
        path = root
        for part in PurePosixPath(name).parts:
            path /= part
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                return
            if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise CacheSecurityError("Cache destination contains a link or special file")

    @staticmethod
    def _digest(stream: IO[bytes]) -> bytes:
        digest = hashlib.sha256()
        while chunk := stream.read(1024**2):
            digest.update(chunk)
        return digest.digest()

    @staticmethod
    def _install(tar: tarfile.TarFile, entry: tarfile.TarInfo, root_fd: int) -> None:
        parts = PurePosixPath(Archiver._member_name(entry.name)).parts
        parent_fd = os.dup(root_fd)
        temporary = None
        try:
            directories = parts if entry.isdir() else parts[:-1]
            for part in directories:
                with suppress(FileExistsError):
                    os.mkdir(part, dir_fd=parent_fd)
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = next_fd
            if entry.isdir():
                return
            source = tar.extractfile(entry)
            if source is None:
                raise CacheSecurityError("Missing cache archive member data")
            with source:
                try:
                    existing_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
                except FileNotFoundError:
                    existing_fd = None
                if existing_fd is not None:
                    with os.fdopen(existing_fd, "rb") as existing:
                        if not stat.S_ISREG(os.fstat(existing.fileno()).st_mode):
                            raise CacheSecurityError("Cache destination is not a regular file")
                        if Archiver._digest(existing) == Archiver._digest(source):
                            return
                    source.seek(0)
                temporary = f".quack-{uuid.uuid4().hex}"
                fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent_fd)
                with os.fdopen(fd, "wb") as output:
                    shutil.copyfileobj(source, output, length=1024**2)
                    os.fchmod(output.fileno(), entry.mode & 0o777)
                os.replace(temporary, parts[-1], src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
                temporary = None
        except OSError as error:
            raise CacheSecurityError("Cache destination changed or cannot be safely installed") from error
        finally:
            if temporary is not None:
                os.unlink(temporary, dir_fd=parent_fd)
            os.close(parent_fd)
