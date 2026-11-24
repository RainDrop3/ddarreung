"""데이터 레이크(S3 호환 스토리지) 접근.

endpoint와 키는 모두 환경변수로 받는다. R2, AWS S3, 로컬 S3를 코드 수정 없이 바꾸기 위함이다.
"""
from __future__ import annotations

import os

import boto3
from botocore.config import Config


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"환경변수 {name}가 설정되지 않았다")
    return value


def bucket_name() -> str:
    return _env("S3_BUCKET")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_env("S3_ENDPOINT_URL"),
        aws_access_key_id=_env("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("S3_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("S3_REGION", "auto"),
        config=Config(signature_version="s3v4"),
    )
