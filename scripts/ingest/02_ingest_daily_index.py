"""
Fetch the SEC EDGAR daily filing index → bronze/daily_index/.

This is the CDC (change data capture) step for the pipeline. Instead of fetching
all ~10k NYSE/Nasdaq CIKs every day, scripts 03 and 04 only process the CIKs
that actually filed something on the target date.

Source:
  https://www.sec.gov/Archives/edgar/daily-index/{YYYY}/QTR{N}/master{YYYYMMDD}.idx
  Pipe-delimited text: CIK|Company Name|Form Type|Date Filed|Filename

Behaviour:
  Default (incremental): fetch today's daily master.idx → intersect with NYSE/Nasdaq
  CIKs from the tickers Parquet → write changed CIKs to bronze/daily_index/.
  Scripts 03 and 04 read from this output.

  --full-refresh: skip the daily index; write ALL NYSE/Nasdaq CIKs to
  bronze/daily_index/. Use for the initial bootstrap and weekly Sunday re-syncs.

Edge cases:
  - Weekend / public holiday: SEC publishes no index → write empty Parquet →
    scripts 03 and 04 detect zero CIKs and exit cleanly.
  - Multiple filings per CIK on one day: deduplicated; forms_filed lists all types.

Output:
  {STORAGE_ROOT}/bronze/daily_index/ingestion_date={date}/data.parquet
  Columns: cik (string, zero-padded to 10), company_name, forms_filed (JSON array),
           ingestion_date, full_refresh (bool)

Usage:
  python scripts/ingest/02_ingest_daily_index.py [--date YYYY-MM-DD] [--full-refresh]
"""
import argparse
import json
import pathlib
import sys
from datetime import date

import pyarrow as pa

_ROOT = pathlib.Path(__file__).parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import settings
from _http import edgar_get_text
from _batch_writer import write_parquet, read_parquet

TARGET_EXCHANGES = {"NYSE", "Nasdaq"}

SCHEMA = pa.schema([
    pa.field("cik",            pa.string()),   # zero-padded 10-digit string
    pa.field("company_name",   pa.string()),
    pa.field("forms_filed",    pa.string()),   # JSON array of form types filed that day
    pa.field("ingestion_date", pa.string()),
    pa.field("full_refresh",   pa.bool_()),
])


def _quarter(d: date) -> int:
    return (d.month - 1) // 3 + 1


def _master_idx_url(d: date) -> str:
    """Daily master.idx URL: pipe-delimited, CIK|Company Name|Form Type|Date Filed|Filename"""
    return (
        f"https://www.sec.gov/Archives/edgar/daily-index"
        f"/{d.year}/QTR{_quarter(d)}/master{d.strftime('%Y%m%d')}.idx"
    )


def _all_nyse_nasdaq_ciks(tickers_path: str) -> dict[str, str]:
    """Return {cik10: company_name} for all NYSE+Nasdaq CIKs."""
    tickers_table = read_parquet(tickers_path)
    d = tickers_table.to_pydict()
    result: dict[str, str] = {}
    for cik_raw, name, exchange in zip(d["cik"], d["name"], d["exchange"]):
        if exchange in TARGET_EXCHANGES:
            cik10 = str(int(cik_raw)).zfill(10)
            result[cik10] = name
    return result


def _parse_master_idx(text: str) -> dict[str, dict]:
    """
    Parse master.idx text into {cik10: {name, forms}}.

    Header format (first few lines, skip until the dashes separator):
      CIK|Company Name|Form Type|Date Filed|Filename
      ---|------------|---------|----------|--------
    Data lines:
      1234567890|APPLE INC|10-K|2024-01-08|edgar/data/.../0001234567890-24-000001.txt
    """
    cik_map: dict[str, dict] = {}
    in_data = False
    for line in text.splitlines():
        stripped = line.strip()
        if not in_data:
            # The separator is a line of dashes (-----...)
            if stripped.startswith("-") and set(stripped) <= {"-", "|", " "}:
                in_data = True
            continue
        parts = stripped.split("|")
        if len(parts) < 5:
            continue
        cik_raw, company_name, form_type = parts[0], parts[1], parts[2]
        if not cik_raw.isdigit():
            continue
        cik10 = cik_raw.zfill(10)
        if cik10 not in cik_map:
            cik_map[cik10] = {"name": company_name, "forms": []}
        if form_type and form_type not in cik_map[cik10]["forms"]:
            cik_map[cik10]["forms"].append(form_type)
    return cik_map


def run(ingest_date: str, full_refresh: bool = False) -> None:
    out_path = (
        f"{settings.STORAGE_ROOT}/bronze/daily_index"
        f"/ingestion_date={ingest_date}/data.parquet"
    )
    tickers_path = (
        f"{settings.STORAGE_ROOT}/bronze/company_tickers_exchange"
        f"/ingestion_date={ingest_date}/data.parquet"
    )

    print(f"[2/4] Building CIK list for {ingest_date} (full_refresh={full_refresh})")

    if full_refresh:
        all_ciks = _all_nyse_nasdaq_ciks(tickers_path)
        records = [
            {
                "cik": cik,
                "company_name": name,
                "forms_filed": json.dumps([]),
                "ingestion_date": ingest_date,
                "full_refresh": True,
            }
            for cik, name in all_ciks.items()
        ]
        print(f"  Full-refresh mode: {len(records):,} NYSE/Nasdaq CIKs")

    else:
        target = date.fromisoformat(ingest_date)
        url = _master_idx_url(target)
        print(f"  Fetching: {url}")
        text = edgar_get_text(url)

        if text is None:
            # Weekend, holiday, or filings not yet published for this date
            print(f"  No daily index for {ingest_date} (weekend/holiday or not yet published)")
            records = []
        else:
            cik_map = _parse_master_idx(text)
            print(f"  Daily index: {len(cik_map):,} unique CIKs filed on {ingest_date}")

            # Intersect with NYSE+Nasdaq universe
            all_nyse_nasdaq = _all_nyse_nasdaq_ciks(tickers_path)
            matched = {cik: info for cik, info in cik_map.items() if cik in all_nyse_nasdaq}
            print(f"  NYSE/Nasdaq intersection: {len(matched):,} CIKs")

            records = [
                {
                    "cik": cik,
                    "company_name": info["name"],
                    "forms_filed": json.dumps(info["forms"]),
                    "ingestion_date": ingest_date,
                    "full_refresh": False,
                }
                for cik, info in matched.items()
            ]

    table = pa.Table.from_pylist(records, schema=SCHEMA)
    write_parquet(table, out_path)
    print(f"  [OK] {len(records):,} CIKs → {out_path}")


def main() -> None:
    import os
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Target date for the daily filing index (default: today)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Skip daily index; process all NYSE/Nasdaq CIKs (bootstrap / weekly re-sync)",
    )
    args = parser.parse_args()
    # Also honour FULL_REFRESH=true env var (set by Step Functions for AWS runs)
    full_refresh = args.full_refresh or os.getenv("FULL_REFRESH", "").lower() == "true"
    run(args.date, full_refresh)


if __name__ == "__main__":
    main()
