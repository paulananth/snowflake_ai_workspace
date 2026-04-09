"""
Ingest SEC EDGAR XBRL company facts -> bronze/companyfacts/.

Reads the CIK list produced by 02_ingest_daily_index.py (incremental mode) or
directly from the tickers Parquet when --full-refresh is passed.

Incremental (default): only fetches CIKs that filed something on the target date,
typically 200-600 companies. Full-refresh: the full ticker-snapshot universe.

All namespaces and all concepts are kept. This is the raw Bronze layer.

Sequential fetching (not parallel): companyfacts payloads are 5-15 MB each;
parallelism adds memory pressure without meaningful throughput gain at 8 req/s.

Resume-safe: skips batches whose output files already exist (idempotent on retry).

Output:
  {STORAGE_ROOT}/bronze/companyfacts/ingestion_date={date}/batch_NNNN.parquet
  Each batch file contains CIKS_PER_BATCH companies worth of fact rows.

Usage:
  python scripts/ingest/04_ingest_companyfacts.py [--date YYYY-MM-DD] [--limit N]
                                                   [--full-refresh]
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
from _rate_limiter import RateLimiter
from _batch_writer import write_parquet, read_parquet, parquet_exists
from models import CompanyFacts, explode_facts
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"

# Flush a Parquet file every N CIKs.
# Companyfacts rows are large (~10k-200k rows/CIK); 100 CIKs keeps files manageable.
CIKS_PER_BATCH = 100

COMPANYFACTS_SCHEMA = pa.schema([
    pa.field("cik",             pa.string()),
    pa.field("entity_name",     pa.string()),
    pa.field("namespace",       pa.string()),   # "us-gaap" | "dei" | "ifrs-full" | ...
    pa.field("concept",         pa.string()),   # all concepts, no filter
    pa.field("label",           pa.string()),
    pa.field("unit",            pa.string()),   # "USD" | "shares" | "pure" | ...
    pa.field("end",             pa.string()),
    pa.field("start",           pa.string()),   # null for instant (balance sheet) facts
    pa.field("val",             pa.float64()),  # Decimal -> float64
    pa.field("accn",            pa.string()),
    pa.field("form",            pa.string()),
    pa.field("filed",           pa.string()),
    pa.field("frame",           pa.string()),   # "CY2023" / "CY2023Q3I" / null
    pa.field("ingestion_date",  pa.string()),
])

_limiter = RateLimiter(rps=8.0)


def _fetch_facts(cik10: str) -> list[dict]:
    _limiter.acquire()
    data = edgar_get(FACTS_URL.format(cik10=cik10))
    if data is None:
        return []
    parsed = CompanyFacts.model_validate(data)
    rows = explode_facts(parsed)
    for row in rows:
        row["val"] = float(row["val"])
    return rows


def _batch_path(ingest_date: str, batch_num: int) -> str:
    return (
        f"{settings.STORAGE_ROOT}/bronze/companyfacts"
        f"/ingestion_date={ingest_date}/batch_{batch_num:04d}.parquet"
    )


def _write_batch(rows: list[dict], batch_num: int, ingest_date: str) -> None:
    for row in rows:
        row["ingestion_date"] = ingest_date
    path = _batch_path(ingest_date, batch_num)
    table = pa.Table.from_pylist(rows, schema=COMPANYFACTS_SCHEMA)
    write_parquet(table, path)
    print(f"  [batch {batch_num:04d}] {len(rows):,} fact rows -> saved")


# CIK source

def _load_ciks(ingest_date: str, full_refresh: bool, limit: int | None) -> list[str]:
    """
    Return the ordered list of CIK10s to process.

    Incremental: read from daily_index parquet (script 02 output).
    Full-refresh: read all ticker-snapshot CIKs from tickers parquet (script 01 output).
    """
    if full_refresh:
        tickers_path = (
            f"{settings.STORAGE_ROOT}/bronze/company_tickers_exchange"
            f"/ingestion_date={ingest_date}/data.parquet"
        )
        tbl = read_parquet(tickers_path)
        d = tbl.to_pydict()
        seen: set[str] = set()
        ciks: list[str] = []
        for cik_raw in d["cik"]:
            cik10 = str(int(cik_raw)).zfill(10)
            if cik10 not in seen:
                seen.add(cik10)
                ciks.append(cik10)
    else:
        daily_path = (
            f"{settings.STORAGE_ROOT}/bronze/daily_index"
            f"/ingestion_date={ingest_date}/data.parquet"
        )
        tbl = read_parquet(daily_path)
        ciks = tbl.column("cik").to_pylist()

    if limit:
        ciks = ciks[:limit]
    return ciks


# Main

def run(ingest_date: str, limit: int | None = None, full_refresh: bool = False) -> None:
    ciks = _load_ciks(ingest_date, full_refresh, limit)

    if not ciks:
        print(f"[4/4] No CIKs to process for {ingest_date} - skipping")
        return

    mode = "full-refresh" if full_refresh else "incremental"
    print(f"[4/4] Fetching companyfacts for {len(ciks):,} CIKs ({mode}, date={ingest_date})")

    # Resume: find the highest batch already written and skip ahead
    batch_num = 1
    while parquet_exists(_batch_path(ingest_date, batch_num)):
        batch_num += 1
    if batch_num > 1:
        skip_ciks = (batch_num - 1) * CIKS_PER_BATCH
        ciks = ciks[skip_ciks:]
        print(f"  Resuming from batch {batch_num} (skipped {skip_ciks} CIKs already written)")

    batch_rows: list[dict] = []
    batch_cik_count = 0
    total_rows = 0
    errors = 0

    for i, cik10 in enumerate(ciks, 1):
        try:
            rows = _fetch_facts(cik10)
        except Exception as exc:
            print(f"  [WARN] CIK {cik10}: {exc}")
            errors += 1
            continue

        if not rows:
            print(f"  [WARN] CIK {cik10}: 404 or empty")
            errors += 1
            continue

        batch_rows.extend(rows)
        batch_cik_count += 1
        total_rows += len(rows)
        print(f"  [{i:,}/{len(ciks):,}] CIK {cik10}: {len(rows):,} rows (total: {total_rows:,})")

        if batch_cik_count >= CIKS_PER_BATCH:
            _write_batch(batch_rows, batch_num, ingest_date)
            batch_rows = []
            batch_cik_count = 0
            batch_num += 1

    if batch_rows:
        _write_batch(batch_rows, batch_num, ingest_date)

    print(f"  Done. Total fact rows: {total_rows:,}  Errors: {errors}")


def main() -> None:
    import os
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max CIKs to process (for dev/smoke testing)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Process all ticker-snapshot CIKs instead of only today's filers",
    )
    args = parser.parse_args()
    # Also honour FULL_REFRESH=true env var (set by Step Functions for AWS runs)
    full_refresh = args.full_refresh or os.getenv("FULL_REFRESH", "").lower() == "true"
    run(args.date, args.limit, full_refresh)


if __name__ == "__main__":
    main()
