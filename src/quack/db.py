#!/usr/bin/env python3

from dataclasses import dataclass
from typing import final

from loguru import logger
from mysql import connector


@dataclass
class TargetChecksum:
    app_name: str
    commit_sha: str
    mr_iid: int
    pipeline_id: int
    job_name: str
    target_name: str
    checksum: str


@final
class DB:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self._db = connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

    def commit(self):
        _ = self._db.commit()

    def close(self):
        self._db.close()

    def record_checksum(self, checksum: TargetChecksum):
        statement = "INSERT INTO quack_target_checksum (app_name, commit_sha, mr_iid, pipeline_id, job_name, target_name, checksum) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE checksum = %s"

        logger.debug(
            "Recording checksum: %s %s %s %s %s %s %s",
            checksum.app_name,
            checksum.commit_sha,
            checksum.mr_iid,
            checksum.pipeline_id,
            checksum.job_name,
            checksum.target_name,
            checksum.checksum,
        )
        with self._db.cursor() as cursor:
            _ = cursor.execute(
                statement,
                (
                    checksum.app_name,
                    checksum.commit_sha,
                    checksum.mr_iid,
                    checksum.pipeline_id,
                    checksum.job_name,
                    checksum.target_name,
                    checksum.checksum,
                    checksum.checksum,
                ),
            )
        self.commit()

    def query_checksum(
        self, app_name: str, commit_sha: str, job_name: str, target_name: str
    ) -> TargetChecksum | None:
        statement = "SELECT app_name, commit_sha, mr_iid, pipeline_id, job_name, target_name, checksum FROM quack_target_checksum WHERE app_name = %s AND commit_sha = %s AND job_name = %s AND target_name = %s;"

        logger.debug(
            "Querying checksum: %s %s %s %s",
            app_name,
            commit_sha,
            job_name,
            target_name,
        )
        with self._db.cursor() as cursor:
            _ = cursor.execute(statement, (app_name, commit_sha, job_name, target_name))
            row = cursor.fetchone()
            if row is None:
                return None
            return TargetChecksum(*row)
