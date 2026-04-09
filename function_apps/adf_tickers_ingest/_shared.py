import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import azure.functions as func

ROOT = Path(__file__).resolve().parent
INGEST_ROOT = ROOT / "scripts" / "ingest"


def load_module(module_name: str, relative_path: str):
    module_path = ROOT / relative_path
    for search_path in (str(ROOT), str(INGEST_ROOT)):
        if search_path not in sys.path:
            sys.path.insert(0, search_path)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_request(req: func.HttpRequest) -> dict:
    try:
        body = req.get_json()
        if isinstance(body, dict):
            return body
    except ValueError:
        pass
    return {}


def resolve_ingest_date(req: func.HttpRequest, payload: dict) -> str:
    ingest_date = (
        req.params.get("ingestDate")
        or payload.get("ingestDate")
        or date.today().isoformat()
    )
    date.fromisoformat(ingest_date)
    return ingest_date


def resolve_bool(req: func.HttpRequest, payload: dict, name: str, default: bool = False) -> bool:
    raw_value = req.params.get(name)
    if raw_value is None:
        raw_value = payload.get(name)
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value

    normalized = str(raw_value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def json_response(status_code: int, payload: dict) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        mimetype="application/json",
        status_code=status_code,
    )
