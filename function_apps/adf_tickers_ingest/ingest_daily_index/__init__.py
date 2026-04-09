import logging

import azure.functions as func

from _shared import json_response, load_module, parse_request, resolve_bool, resolve_ingest_date


def main(req: func.HttpRequest) -> func.HttpResponse:
    payload = parse_request(req)

    try:
        ingest_date = resolve_ingest_date(req, payload)
        full_refresh = resolve_bool(req, payload, "fullRefresh", False)
    except ValueError as exc:
        return json_response(
            400,
            {
                "status": "Failed",
                "error": str(exc),
            },
        )

    try:
        daily_index = load_module(
            "ingest_daily_index_entrypoint",
            "scripts/ingest/02_ingest_daily_index.py",
        )
        output_path = daily_index.run(ingest_date, full_refresh)
        logging.info(
            "Daily index ingest completed for %s (fullRefresh=%s)",
            ingest_date,
            full_refresh,
        )
        return json_response(
            200,
            {
                "status": "Succeeded",
                "ingestDate": ingest_date,
                "fullRefresh": full_refresh,
                "outputPath": output_path,
            },
        )
    except Exception as exc:
        logging.exception("Daily index ingest failed")
        return json_response(
            500,
            {
                "status": "Failed",
                "ingestDate": ingest_date,
                "fullRefresh": full_refresh,
                "error": str(exc),
            },
        )
