import importlib.util
import json
import logging
import sys
from datetime import date
from pathlib import Path

import azure.functions as func

_ROOT = Path(__file__).resolve().parents[1]
_INGEST_ROOT = _ROOT / "scripts" / "ingest"


def _load_module(module_name: str, relative_path: str):
    module_path = _ROOT / relative_path
    for search_path in (str(_ROOT), str(_INGEST_ROOT)):
        if search_path not in sys.path:
            sys.path.insert(0, search_path)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _parse_request(req: func.HttpRequest) -> dict:
    try:
        body = req.get_json()
        if isinstance(body, dict):
            return body
    except ValueError:
        pass
    return {}


def _resolve_ingest_date(req: func.HttpRequest, payload: dict) -> str:
    ingest_date = (
        req.params.get("ingestDate")
        or payload.get("ingestDate")
        or date.today().isoformat()
    )
    date.fromisoformat(ingest_date)
    return ingest_date


def _json_response(status_code: int, payload: dict) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        mimetype="application/json",
        status_code=status_code,
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    payload = _parse_request(req)

    try:
        ingest_date = _resolve_ingest_date(req, payload)
    except ValueError:
        return _json_response(
            400,
            {
                "status": "Failed",
                "error": "ingestDate must be YYYY-MM-DD",
            },
        )

    try:
        tickers_exchange = _load_module(
            "ingest_tickers_exchange_entrypoint",
            "scripts/ingest/01_ingest_tickers_exchange.py",
        )
        output_path = tickers_exchange.run(ingest_date)
        logging.info("Ticker ingest completed for %s", ingest_date)
        return _json_response(
            200,
            {
                "status": "Succeeded",
                "ingestDate": ingest_date,
                "outputPath": output_path,
            },
        )
    except Exception as exc:
        logging.exception("Ticker ingest failed")
        return _json_response(
            500,
            {
                "status": "Failed",
                "ingestDate": ingest_date,
                "error": str(exc),
            },
        )
