import json
import os
from datetime import datetime, timezone
from utils.config import PIPELINE_STATE_PATH, STATE_PATH



def _read(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)


def _write(path: str, payload: dict) -> None:
    with open(path, "wb") as f:
        f.write(json.dumps(payload).encode("utf-8"))


def get_pipeline_state() -> dict:
    state = _read(PIPELINE_STATE_PATH)
    if state is None:
        reset_pipeline_halt()
        return get_pipeline_state()
    return state


def is_pipeline_halted() -> bool:
    return get_pipeline_state()["pipeline_halted"]


def set_pipeline_halted(reason: str) -> None:
    _write(PIPELINE_STATE_PATH, {
        "pipeline_halted": True,
        "halted_since": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "halted_reason": reason,
        "insee_delta_cursor": get_insee_delta_cursor()
    })


def reset_pipeline_halt() -> None:
    os.makedirs(STATE_PATH, exist_ok=True)
    insee_delta_cursor  = get_insee_delta_cursor()
    _write(PIPELINE_STATE_PATH, {
        "pipeline_halted": False,
        "halted_since": None,
        "halted_reason": None,
        "insee_delta_cursor": insee_delta_cursor
    })


def set_insee_delta_cursor (date: str) -> None:
    state = get_pipeline_state()
    state["insee_delta_cursor"] = date
    _write(PIPELINE_STATE_PATH, state)
    print(f"New insee delta cursor set : {date}")


def get_insee_delta_cursor () -> str | None:
    return get_pipeline_state().get("insee_delta_cursor")