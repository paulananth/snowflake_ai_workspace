"""
Ingest SEC EDGAR XBRL company facts → bronze/companyfacts/.

Reads CIK list from the tickers Parquet produced by 01_ingest_tickers_exchange.py.
Filters to NYSE and Nasdaq companies.

ALL namespaces (us-gaap, dei, ifrs-full, ...), ALL concepts — no filter.
This is the raw Bronze layer: every tagged XBRL value from every filing.

Sequential fetching (not parallel): companyfacts payloads are 5-15 MB each;
parallelism adds memory pressure without meaningful throughput gain at 8 req/s.

Output:
  {STORAGE_ROOT}/bronze/companyfacts/ingestion_date={date}/batch_NNNN.parquet
  Each batch file contains CIKS_PER_BATCH companies' worth of fact rows.

Usage:
  python scripts/ingest/03_ingest_companyfacts.py [--date YYYY-MM-DD] [--limit N]
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
from _batch_writer import write_parquet, read_parquet
from models import CompanyFacts, explode_facts

TARGET_EXCHANGES = {"NYSE", "Nasdaq"}
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
    pa.field("val",             pa.float64()),  # Decimal → float64
    pa.field("accn",            pa.string()),
    pa.field("form",            pa.string()),
    pa.field("filed",           pa.string()),
    pa.field("frame",           pa.string()),   # "CY2023" / "CY2023Q3I" / null
    pa.field("ingestion_date",  pa.string()),
])

_limiter = RateLimiter(rps=8.0)


def _fetch_facts(cik10: str) -> list[dict]:
    """Rate-limited fetch + explode for one CIK. Returns flat fact rows."""
    _limiter.acquire()
    data = edgar_get(FACTS_URL.format(cik10=cik10))
    if data is None:
        return []
    parsed = CompanyFacts.model_validate(data)
    rows = explode_facts(parsed)
    # Pydantic stores val as Decimal; PyArrow float64 does not accept Decimal directly
    for row in rows:
        row["val"] = float(row["val"])
    return rows


def _write_batch(rows: list[dict], batch_num: int, ingest_date: str) -> None:
    for row in rows:
        row["ingestion_date"] = ingest_date
    path = (
        f"{settings.STORAGE_ROOT}/bronze/companyfacts"
        f"/ingestion_date={ingest_date}/batch_{batch_num:04d}.parquet"
    )
    table = pa.Table.from_pylist(rows, schema=COMPANYFACTS_SCHEMA)
    write_parquet(table, path)
    print(f"  [batch {batch_num:04d}] {len(rows):,} fact rows → saved")


def run(ingest_date: str, limit: int | None = None) -> None:
    tickers_path = (
        f"{settings.STORAGE_ROOT}/bronze/company_tickers_exchange"
        f"/ingestion_date={ingest_date}/data.parquet"
    )
    tickers_table = read_parquet(tickers_path)
    tickers_dict = tickers_table.to_pydict()

    # Deduplicate CIKs; filter to NYSE + Nasdaq
    seen: set[str] = set()
    ciks: list[str] = []
    for cik_raw, exchange in zip(tickers_dict["cik"], tickers_dict["exchange"]):
        cik10 = str(int(cik_raw)).zfill(10)
        if exchange in TARGET_EXCHANGES and cik10 not in seen:
            seen.add(cik10)
            ciks.append(cik10)

    if limit:
        ciks = ciks[:limit]

    print(f"[3/3] Fetching companyfacts for {len(ciks):,} CIKs (date={ingest_date})")

    batch_rows: list[dict] = []
    batch_cik_count = 0
    batch_num = 1
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
    args = parser.parse_args()
    run(args.date, args.limit)


if __name__ == "__main__":
    main()
