import boto3
from botocore.config import Config
import json
from datetime import datetime, timezone
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")


def get_r2_client(dbutils):
    """Initialize and return a boto3 S3 client configured for Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=dbutils.secrets.get(scope="r2-insee-sirene-monitor-dlz-credentials", key="access_key"),
        aws_secret_access_key=dbutils.secrets.get(scope="r2-insee-sirene-monitor-dlz-credentials", key="secret_key"),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3}
        )
    )

BUCKET = "insee-sirene-monitor-data-landing-zone"
LAST_RUN_KEY = "last_run.json"

def get_last_run(client) -> dict | None:
    """
    Retrieve last run metadata from R2.
    Returns None if first run.
    """
    try:
        response = client.get_object(Bucket=BUCKET, Key=LAST_RUN_KEY)
        return json.loads(response["Body"].read().decode("utf-8"))
    except client.exceptions.NoSuchKey:
        return None

def set_last_run(client, run_date: datetime) -> None:
    """
    Store last run metadata in R2.
    """
    payload = {
        "last_run": run_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    client.put_object(
        Bucket=BUCKET,
        Key=LAST_RUN_KEY,
        Body=json.dumps(payload).encode("utf-8")
    )

def is_first_run(client) -> bool:
    """Check if this is the first pipeline run."""
    return get_last_run(client) is None