#!/usr/bin/env python3

import os
from functools import cached_property


class CIEnvironment:
    @cached_property
    def is_ci(self) -> bool:
        return os.environ.get("CI", "false").lower() == "true"

    @cached_property
    def pr_id(self) -> int:
        gitlab_pr_id = os.environ.get("CI_MERGE_REQUEST_IID")
        github_pr_id = os.environ.get("GITHUB_PULL_REQUEST_ID")

        return int(gitlab_pr_id or github_pr_id or 0)

    @cached_property
    def pipeline_id(self) -> int:
        gitlab_pipeline_id = os.environ.get("CI_PIPELINE_ID")
        github_pipeline_id = os.environ.get("GITHUB_RUN_ID")

        return int(gitlab_pipeline_id or github_pipeline_id or 0)

    @cached_property
    def job_name(self) -> str:
        gitlab_job_name = os.environ.get("CI_JOB_NAME")
        github_job_name = os.environ.get("GITHUB_JOB")

        return gitlab_job_name or github_job_name or ""

    @cached_property
    def commit_sha(self) -> str:
        gitlab_commit_sha = os.environ.get("CI_COMMIT_SHA")
        github_commit_sha = os.environ.get("GITHUB_SHA")

        return gitlab_commit_sha or github_commit_sha or ""

    @cached_property
    def is_merge_group(self) -> bool:
        gitlab_is_merge_train = (
            os.environ.get("CI_MERGE_REQUEST_EVENT_TYPE") == "merge_train"
        )
        github_is_merge_group = os.environ.get("GITHUB_EVENT_NAME") == "merge_group"

        return gitlab_is_merge_train or github_is_merge_group
