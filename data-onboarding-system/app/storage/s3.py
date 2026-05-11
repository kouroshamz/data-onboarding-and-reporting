"""S3 artifact storage.

Uploads pipeline artifacts to an S3 bucket, mirroring the local storage
layout: ``s3://<bucket>/<prefix>/<run_id>/<artifact>``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment,misc]


class S3Storage:
    """Upload / download pipeline artifacts to S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "onboarding-runs",
        region: str = "us-east-1",
        **boto_kwargs: Any,
    ):
        if boto3 is None:
            raise ImportError("boto3 is required for S3Storage – pip install boto3")
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self._s3 = boto3.client("s3", region_name=region, **boto_kwargs)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upload_json(self, run_id: str, name: str, data: Any) -> str:
        """Upload JSON data. Returns the S3 key."""
        key = f"{self.prefix}/{run_id}/{name}.json"
        body = json.dumps(data, indent=2, default=str)
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
        logger.debug("Uploaded JSON to s3://{}/{}", self.bucket, key)
        return key

    def upload_file(self, run_id: str, local_path: str | Path) -> str:
        """Upload a local file. Returns the S3 key."""
        p = Path(local_path)
        key = f"{self.prefix}/{run_id}/{p.name}"
        self._s3.upload_file(str(p), self.bucket, key)
        logger.debug("Uploaded file to s3://{}/{}", self.bucket, key)
        return key

    def upload_bytes(self, run_id: str, name: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        key = f"{self.prefix}/{run_id}/{name}"
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        return key

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def download_json(self, run_id: str, name: str) -> Any:
        key = f"{self.prefix}/{run_id}/{name}.json"
        resp = self._s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(resp["Body"].read().decode())

    def list_runs(self) -> List[str]:
        """List run IDs in the bucket prefix."""
        paginator = self._s3.get_paginator("list_objects_v2")
        runs: set[str] = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix + "/", Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"].rstrip("/").split("/")[-1]
                runs.add(name)
        return sorted(runs)

    def list_artifacts(self, run_id: str) -> List[str]:
        """List artifact keys for a specific run."""
        prefix = f"{self.prefix}/{run_id}/"
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: List[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"].split("/")[-1])
        return keys
