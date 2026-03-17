import json
from datetime import datetime, timezone
from utils.config import BUCKET, PIPELINE_STATE_KEY, BATCH_STAGE_KEY


def get_pipeline_state(client) -> dict:
    """Retrieve pipeline state from R2. Initializes default state if not found."""
    try:
        response = client.get_object(Bucket=BUCKET, Key=PIPELINE_STATE_KEY)
        return json.loads(response["Body"].read().decode("utf-8"))
    except client.exceptions.NoSuchKey:
        reset_pipeline_halt(client)
        return get_pipeline_state(client)


def is_pipeline_halted(client) -> bool:
    """Check if pipeline is halted."""
    return get_pipeline_state(client)["pipeline_halted"]


def set_pipeline_halted(client, reason: str) -> None:
    """Set pipeline halted flag with reason."""
    payload = {
        "pipeline_halted": True,
        "halted_since": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "halted_reason": reason
    }
    client.put_object(
        Bucket=BUCKET,
        Key=PIPELINE_STATE_KEY,
        Body=json.dumps(payload).encode("utf-8")
    )


def reset_pipeline_halt(client) -> None:
    """Reset pipeline halted flag."""
    payload = {
        "pipeline_halted": False,
        "halted_since": None,
        "halted_reason": None
    }
    client.put_object(
        Bucket=BUCKET,
        Key=PIPELINE_STATE_KEY,
        Body=json.dumps(payload).encode("utf-8")
    )


def set_batch_stage(client, batch_date: str, stage: str) -> None:
    """Record current processing stage for a batch."""
    payload = {
        "batch_date": batch_date,
        "stage": stage,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    client.put_object(
        Bucket=BUCKET,
        Key=BATCH_STAGE_KEY,
        Body=json.dumps(payload).encode("utf-8")
    )


def get_batch_stage(client) -> dict | None:
    """Retrieve current batch processing stage from R2."""
    try:
        response = client.get_object(Bucket=BUCKET, Key=BATCH_STAGE_KEY)
        return json.loads(response["Body"].read().decode("utf-8"))
    except client.exceptions.NoSuchKey:
        return None