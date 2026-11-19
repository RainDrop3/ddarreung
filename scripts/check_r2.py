"""R2(S3 호환) 연결 확인: 업로드 → 목록 조회 → 읽기 → 삭제.

endpoint·키는 모두 환경변수(.env)에서 읽는다. 코드에는 R2 전용 로직이 없다.
실행: python scripts/check_r2.py
"""
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

REQUIRED = ["S3_ENDPOINT_URL", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_BUCKET"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    sys.exit(f".env에 값이 없음: {', '.join(missing)}")

bucket = os.environ["S3_BUCKET"]
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    region_name=os.getenv("S3_REGION", "auto"),
    config=Config(signature_version="s3v4"),
)

key = "_healthcheck/ping.txt"
body = f"ok {datetime.now(timezone.utc).isoformat()}".encode()

s3.put_object(Bucket=bucket, Key=key, Body=body)
print(f"[PUT]  s3://{bucket}/{key}")

listed = s3.list_objects_v2(Bucket=bucket, Prefix="_healthcheck/")
print("[LIST]", [o["Key"] for o in listed.get("Contents", [])])

got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
assert got == body, "읽은 내용이 쓴 내용과 다름"
print("[GET]  내용 일치")

s3.delete_object(Bucket=bucket, Key=key)
print("[DEL]  정리 완료 — R2 연결 정상")
