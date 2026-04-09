"""
Ingest SEC EDGAR ticker-exchange snapshot -> bronze/company_tickers_exchange/.

Fetches https://www.sec.gov/files/company_tickers_exchange.json (one HTTP call).
Writes all rows with no exchange filter as raw Bronze output.

Output:
  {STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={date}/data.parquet

Idempotent: skips the fetch if the local output file already exists.

Usage:
  python scripts/ingest/01_ingest_tickers_exchange.py [--date YYYY-MM-DD]
"""
import argparse
import pathlib
import sys
from datetime import date

import pyarrow as pa

_ROOT = pathlib.Path(__file__).parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import settings
from _http import edgar_get
from _batch_writer import write_parquet

URL = "https://www.sec.gov/files/company_tickers_exchange.json"

SCHEMA = pa.schema([
    pa.field("cik",      pa.int64()),
    pa.field("name",     pa.string()),
    pa.field("ticker",   pa.string()),
    pa.field("exchange", pa.string()),
])


def run(ingest_date: str) -> str:
    out_path = (
        f"{settings.STORAGE_ROOT}/bronze/company_tickers_exchange"
        f"/ingestion_date={ingest_date}/data.parquet"
    )

    # Idempotency guard (local only - Azure writes are always safe to overwrite).
    if settings.CLOUD_PROVIDER == "local" and pathlib.Path(out_path).exists():
        print(f"[SKIP] {out_path} already exists")
        return out_path

    print("[1/3] Fetching ticker exchange snapshot...")
    data = edgar_get(URL)
    if data is None:
        raise RuntimeError("company_tickers_exchange.json returned 404")

    fields = data.get("fields", [])
    rows = data.get("data", [])
    records = [dict(zip(fields, row)) for row in rows]

    table = pa.Table.from_pylist(records, schema=SCHEMA)
    write_parquet(table, out_path)
    print(f"  [OK] {len(records):,} rows -> {out_path}")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Ingestion date partition (default: today)",
    )
    args = parser.parse_args()
    run(args.date)


if __name__ == "__main__":
    main()
