import json
import os
from datetime import datetime, timezone
from utils.config import PIPELINE_STATE_PATH, STATE_PATH


def _read(path: str) -> dict | None:
    """Read and deserialize a JSON file from the given path.
    Returns None if the file does not exist."""

    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)


def _write(path: str, payload: dict) -> None:
    """Serialize and write a dict as JSON to the given path."""

    with open(path, "wb") as f:
        f.write(json.dumps(payload).encode("utf-8"))


def get_pipeline_state() -> dict:
    """Read the pipeline state from the state file. If the file does not exist,
    initializes it with a default reset state and returns it."""

    state = _read(PIPELINE_STATE_PATH)
    if state is None:
        reset_pipeline_halt()
        return get_pipeline_state()
    return state


def is_pipeline_halted() -> bool:
    """Return True if the pipeline circuit breaker is currently set."""

    return get_pipeline_state()["pipeline_halted"]


def set_pipeline_halted(reason: str) -> None:
    """Set the pipeline circuit breaker with a reason and timestamp.
    Preserves the current insee_delta_cursor value."""

    _write(PIPELINE_STATE_PATH, {
        "pipeline_halted": True,
        "halted_since": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "halted_reason": reason,
        "insee_delta_cursor": get_insee_delta_cursor()
    })


def reset_pipeline_halt() -> None:
    """Clear the pipeline circuit breaker. Creates the state directory if needed.
    Preserves the current insee_delta_cursor value if one exists."""

    os.makedirs(STATE_PATH, exist_ok=True)
    insee_delta_cursor = get_insee_delta_cursor()
    _write(PIPELINE_STATE_PATH, {
        "pipeline_halted": False,
        "halted_since": None,
        "halted_reason": None,
        "insee_delta_cursor": insee_delta_cursor
    })


def set_insee_delta_cursor(date: str) -> None:
    """Update the INSEE delta cursor in the pipeline state file.
    The cursor is used as the lower bound date filter for the next API delta call."""

    state = get_pipeline_state()
    state["insee_delta_cursor"] = date
    _write(PIPELINE_STATE_PATH, state)
    print(f"New INSEE delta cursor set: {date}", flush=True)


def get_insee_delta_cursor() -> str | None:
    """Return the current INSEE delta cursor from the pipeline state,
    or None if not yet set."""

    return get_pipeline_state().get("insee_delta_cursor")