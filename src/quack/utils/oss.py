#!/usr/bin/env python3

import subprocess
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union


class OSSError(Exception):
    def __init__(self, message, stdout, stderr):
        self.message = message
        self.stdout = stdout
        self.stderr = stderr


@dataclass
class OSSFileMetadata:
    path: str
    modified_time: datetime
    size: int


class OSSClient:
    def __init__(
        self,
        prefix: str,
        config_file: Union[str, None] = None,
        ak: Union[str, None] = None,
        sk: Union[str, None] = None,
        endpoint: Union[str, None] = None,
        log_level="info",
        parallel_level=50,
    ):
        self._prefix = prefix
        self._log_level = log_level
        self._parallel_level = parallel_level

        if config_file is None:
            self.oss_command = (
                f"ossutil64 -i {ak} -k {sk} -e {endpoint} --loglevel {self._log_level}"
            )
        else:
            self.oss_command = (
                f"ossutil64 -c {config_file} --loglevel {self._log_level}"
            )

    @staticmethod
    def _parse_datetime(datetime_str: str) -> datetime:
        format_string = "%Y-%m-%d %H:%M:%S %z %Z"
        return datetime.strptime(datetime_str, format_string).replace(tzinfo=None)

    def filter_files(
        self, path: str, include: List[str], exclude: List[str]
    ) -> List[OSSFileMetadata]:
        cmd = f"{self.oss_command} ls {self._prefix}/{path}"
        for pattern in include:
            cmd += f" --include {pattern}"
        for pattern in exclude:
            cmd += f" --exclude {pattern}"

        try:
            p = subprocess.check_output(cmd, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            raise OSSError(f"列出文件失败：{path}", e.stdout, e.stderr)

        result = []
        for line in p.splitlines()[2:]:
            parts = line.rsplit(None, 4)
            if len(parts) < 5:
                continue
            result.append(
                OSSFileMetadata(
                    path=parts[4][len(self._prefix) + 1 :],
                    modified_time=self._parse_datetime(parts[0]),
                    size=int(parts[1]),
                )
            )
        return result

    def exists(self, path: str) -> bool:
        cmd = f"{self.oss_command} stat {self._prefix}/{path} >/dev/null 2>&1"
        p = subprocess.run(cmd, shell=True)
        return p.returncode == 0

    def upload(self, path: str, dest: str, force=False) -> None:
        copy_param = "-f" if force else "-u"
        cmd = f"{self.oss_command} cp --jobs {self._parallel_level} {copy_param} {path} {self._prefix}/{dest}"
        try:
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise OSSError(f"上传文件失败：{path}", e.stdout, e.stderr)

    def download(self, path: str, dest: str) -> None:
        cmd = f"{self.oss_command} cp --jobs {self._parallel_level} -u {self._prefix}/{path} {dest}"
        try:
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise OSSError(f"下载文件失败：{path}", e.stdout, e.stderr)

    def remove(self, path: str, recursive=False) -> None:
        remove_param = "-rf" if recursive else "-f"
        cmd = f"{self.oss_command} rm {remove_param} {self._prefix}/{path}"
        try:
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise OSSError(f"删除文件失败：{path}", e.stdout, e.stderr)
