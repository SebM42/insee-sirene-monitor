import boto3
from botocore.config import Config
import os
from pathlib import Path
from dotenv import load_dotenv
from utils.config import R2_SCOPE, INSEE_API_SCOPE

load_dotenv(Path(__file__).parent.parent / ".env")

def get_r2_client(dbutils):
    """Initialize and return a boto3 S3 client configured for Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=dbutils.secrets.get(scope=R2_SCOPE, key="access_key"),
        aws_secret_access_key=dbutils.secrets.get(scope=R2_SCOPE, key="secret_key"),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3}
        )
    )

def get_insee_api_key(dbutils) -> str:
    """Retrieve INSEE API key from vault."""
    return dbutils.secrets.get(
        scope=INSEE_API_SCOPE,
        key="api_key"
    )