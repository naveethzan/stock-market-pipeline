import boto3
from botocore.exceptions import ClientError
from typing import Optional
import datetime


def s3_client():
    return boto3.client('s3')


def put_bytes(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    client = s3_client()
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def write_success_marker(bucket: str, prefix: str, content: Optional[bytes] = None) -> None:
    if not prefix.endswith('/'):
        prefix += '/'
    key = f"{prefix}_SUCCESS"
    body = content if content is not None else f"success at {datetime.datetime.utcnow().isoformat()}Z".encode()
    put_bytes(bucket=bucket, key=key, data=body, content_type="text/plain")


def copy_object(bucket: str, source_key: str, dest_key: str) -> None:
    """Server-side copy within the same bucket."""
    client = s3_client()
    client.copy({'Bucket': bucket, 'Key': source_key}, bucket, dest_key)


def delete_object(bucket: str, key: str) -> None:
    """Delete an object by key."""
    client = s3_client()
    client.delete_object(Bucket=bucket, Key=key)
