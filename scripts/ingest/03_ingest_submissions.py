"""
Ingest SEC EDGAR company submissions → bronze/submissions_meta/ + bronze/filings_index/.

Reads the CIK list produced by 02_ingest_daily_index.py (incremental mode) or
directly from the tickers Parquet when --full-refresh is passed.

Incremental (default): only fetches CIKs that filed something on the target date,
typically 200-600 companies. Full-refresh: all ~5k NYSE/Nasdaq CIKs (~90 min).

Produces two Parquet outputs:
  submissions_meta  — one row per company (all Submission fields, addresses flattened)
  filings_index     — one row per filing  (exploded from filings.recent, all fields)

Writes batches of BATCH_SIZE CIKs to separate numbered Parquet files.
Resume-safe: skips batches whose output files already exist (idempotent on retry).

Usage:
  python scripts/ingest/03_ingest_submissions.py [--date YYYY-MM-DD] [--limit N]
                                                  [--full-refresh]
"""
import argparse
import json
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pyarrow as pa

_ROOT = pathlib.Path(__file__).parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import settings
from _http import edgar_get
from _rate_limiter import RateLimiter
from _batch_writer import write_parquet, read_parquet, parquet_exists
from models import Submission, explode_filings

TARGET_EXCHANGES = {"NYSE", "Nasdaq"}
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"

# ── PyArrow schemas ───────────────────────────────────────────────────────────

SUBMISSIONS_META_SCHEMA = pa.schema([
    pa.field("cik",                                 pa.string()),
    pa.field("name",                                pa.string()),
    pa.field("entityType",                          pa.string()),
    pa.field("sic",                                 pa.string()),
    pa.field("sicDescription",                      pa.string()),
    pa.field("ownerOrg",                            pa.string()),
    pa.field("insiderTransactionForOwnerExists",     pa.int64()),
    pa.field("insiderTransactionForIssuerExists",    pa.int64()),
    pa.field("tickers",                             pa.string()),   # JSON array
    pa.field("exchanges",                           pa.string()),   # JSON array
    pa.field("ein",                                 pa.string()),
    pa.field("lei",                                 pa.string()),
    pa.field("description",                         pa.string()),
    pa.field("website",                             pa.string()),
    pa.field("investorWebsite",                     pa.string()),
    pa.field("category",                            pa.string()),
    pa.field("fiscalYearEnd",                       pa.string()),
    pa.field("stateOfIncorporation",                pa.string()),
    pa.field("stateOfIncorporationDescription",     pa.string()),
    pa.field("phone",                               pa.string()),
    pa.field("flags",                               pa.string()),
    pa.field("formerNames",                         pa.string()),   # JSON array
    pa.field("filing_files_count",                  pa.int64()),
    # Mailing address (6 columns)
    pa.field("addr_mailing_street1",                pa.string()),
    pa.field("addr_mailing_street2",                pa.string()),
    pa.field("addr_mailing_city",                   pa.string()),
    pa.field("addr_mailing_stateOrCountry",         pa.string()),
    pa.field("addr_mailing_zipCode",                pa.string()),
    pa.field("addr_mailing_stateOrCountryDescription", pa.string()),
    # Business address (6 columns)
    pa.field("addr_business_street1",               pa.string()),
    pa.field("addr_business_street2",               pa.string()),
    pa.field("addr_business_city",                  pa.string()),
    pa.field("addr_business_stateOrCountry",        pa.string()),
    pa.field("addr_business_zipCode",               pa.string()),
    pa.field("addr_business_stateOrCountryDescription", pa.string()),
    pa.field("ingestion_date",                      pa.string()),
])

FILINGS_INDEX_SCHEMA = pa.schema([
    pa.field("cik",                     pa.string()),
    pa.field("accessionNumber",         pa.string()),
    pa.field("filingDate",              pa.string()),
    pa.field("reportDate",              pa.string()),
    pa.field("acceptanceDateTime",      pa.string()),
    pa.field("act",                     pa.string()),
    pa.field("form",                    pa.string()),
    pa.field("fileNumber",              pa.string()),
    pa.field("filmNumber",              pa.string()),
    pa.field("items",                   pa.string()),
    pa.field("core_type",               pa.string()),
    pa.field("size",                    pa.int64()),
    pa.field("isXBRL",                  pa.int64()),
    pa.field("isXBRLNumeric",           pa.int64()),
    pa.field("isInlineXBRL",            pa.int64()),
    pa.field("primaryDocument",         pa.string()),
    pa.field("primaryDocDescription",   pa.string()),
    pa.field("ingestion_date",          pa.string()),
])

# ── Helpers ───────────────────────────────────────────────────────────────────

_limiter = RateLimiter(rps=8.0)

_ADDR_FIELDS = [
    "street1", "street2", "city",
    "stateOrCountry", "zipCode", "stateOrCountryDescription",
]


def _address_columns(addresses: dict | None, addr_type: str) -> dict:
    prefix = f"addr_{addr_type}_"
    result = {f"{prefix}{f}": None for f in _ADDR_FIELDS}
    if addresses:
        addr = addresses.get(addr_type)
        if addr:
            for f in _ADDR_FIELDS:
                result[f"{prefix}{f}"] = getattr(addr, f, None)
    return result


def _to_meta_row(sub: Submission, ingest_date: str) -> dict:
    row: dict = {
        "cik":                               sub.cik,
        "name":                              sub.name,
        "entityType":                        sub.entityType,
        "sic":                               sub.sic,
        "sicDescription":                    sub.sicDescription,
        "ownerOrg":                          sub.ownerOrg,
        "insiderTransactionForOwnerExists":  sub.insiderTransactionForOwnerExists,
        "insiderTransactionForIssuerExists": sub.insiderTransactionForIssuerExists,
        "tickers":                           json.dumps(sub.tickers),
        "exchanges":                         json.dumps(sub.exchanges),
        "ein":                               sub.ein,
        "lei":                               sub.lei,
        "description":                       sub.description,
        "website":                           sub.website,
        "investorWebsite":                   sub.investorWebsite,
        "category":                          sub.category,
        "fiscalYearEnd":                     sub.fiscalYearEnd,
        "stateOfIncorporation":              sub.stateOfIncorporation,
        "stateOfIncorporationDescription":   sub.stateOfIncorporationDescription,
        "phone":                             sub.phone,
        "flags":                             sub.flags,
        "formerNames":                       json.dumps(
            [fn.model_dump(by_alias=True) for fn in sub.formerNames]
        ),
        "filing_files_count":                len(sub.filing_files),
        "ingestion_date":                    ingest_date,
    }
    row.update(_address_columns(sub.addresses, "mailing"))
    row.update(_address_columns(sub.addresses, "business"))
    return row


def _fetch_one(cik10: str) -> tuple[Submission | None, list[dict]]:
    _limiter.acquire()
    data = edgar_get(SUBMISSIONS_URL.format(cik10=cik10))
    if data is None:
        return None, []
    data["cik"] = cik10
    sub = Submission.model_validate(data)
    recent_raw = sub.filings_recent.model_dump() if sub.filings_recent else {}
    filing_rows = explode_filings(cik10, recent_raw)
    return sub, filing_rows


def _batch_path(prefix: str, ingest_date: str, batch_num: int) -> tuple[str, str]:
    label = f"batch_{batch_num:04d}.parquet"
    meta = f"{prefix}/submissions_meta/ingestion_date={ingest_date}/{label}"
    filings = f"{prefix}/filings_index/ingestion_date={ingest_date}/{label}"
    return meta, filings


def _write_batch(
    meta_subs: list[Submission],
    filing_rows: list[dict],
    batch_num: int,
    ingest_date: str,
) -> None:
    meta_path, filings_path = _batch_path(settings.STORAGE_ROOT, ingest_date, batch_num)

    meta_records = [_to_meta_row(sub, ingest_date) for sub in meta_subs]
    write_parquet(pa.Table.from_pylist(meta_records, schema=SUBMISSIONS_META_SCHEMA), meta_path)

    for row in filing_rows:
        row["ingestion_date"] = ingest_date
    write_parquet(
        pa.Table.from_pylist(filing_rows, schema=FILINGS_INDEX_SCHEMA), filings_path
    )

    print(
        f"  [batch {batch_num:04d}] "
        f"{len(meta_records)} companies, {len(filing_rows):,} filings → saved"
    )


# ── CIK source ────────────────────────────────────────────────────────────────

def _load_ciks(ingest_date: str, full_refresh: bool, limit: int | None) -> list[str]:
    """
    Return the ordered list of CIK10s to process.

    Incremental: read from daily_index parquet (script 02 output).
    Full-refresh: read all NYSE/Nasdaq CIKs from tickers parquet (script 01 output).
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
        for cik_raw, exchange in zip(d["cik"], d["exchange"]):
            if exchange in TARGET_EXCHANGES:
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


# ── Main ──────────────────────────────────────────────────────────────────────

def run(ingest_date: str, limit: int | None = None, full_refresh: bool = False) -> None:
    ciks = _load_ciks(ingest_date, full_refresh, limit)

    if not ciks:
        print(f"[3/4] No CIKs to process for {ingest_date} — skipping")
        return

    mode = "full-refresh" if full_refresh else "incremental"
    print(f"[3/4] Fetching submissions for {len(ciks):,} CIKs ({mode}, date={ingest_date})")

    batch_subs: list[Submission] = []
    batch_filings: list[dict] = []
    batch_num = 1
    errors = 0

    # Resume: find the highest batch already written and skip ahead
    while True:
        meta_path, _ = _batch_path(settings.STORAGE_ROOT, ingest_date, batch_num)
        if parquet_exists(meta_path):
            batch_num += 1
        else:
            break
    if batch_num > 1:
        skip_ciks = (batch_num - 1) * settings.BATCH_SIZE
        ciks = ciks[skip_ciks:]
        print(f"  Resuming from batch {batch_num} (skipped {skip_ciks} CIKs already written)")

    with ThreadPoolExecutor(max_workers=settings.INGEST_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, cik10): cik10 for cik10 in ciks}
        for i, future in enumerate(as_completed(futures), 1):
            cik10 = futures[future]
            try:
                sub, filings = future.result()
            except Exception as exc:
                print(f"  [WARN] CIK {cik10}: {exc}")
                errors += 1
                continue
            if sub is None:
                print(f"  [WARN] CIK {cik10}: 404")
                errors += 1
                continue

            batch_subs.append(sub)
            batch_filings.extend(filings)

            if len(batch_subs) >= settings.BATCH_SIZE:
                _write_batch(batch_subs, batch_filings, batch_num, ingest_date)
                batch_subs, batch_filings = [], []
                batch_num += 1

            if i % 500 == 0:
                print(f"  {i:,}/{len(ciks):,} fetched...")

    if batch_subs:
        _write_batch(batch_subs, batch_filings, batch_num, ingest_date)

    print(f"  Done. Errors: {errors}")


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
        help="Process all NYSE/Nasdaq CIKs instead of only today's filers",
    )
    args = parser.parse_args()
    # Also honour FULL_REFRESH=true env var (set by Step Functions for AWS runs)
    full_refresh = args.full_refresh or os.getenv("FULL_REFRESH", "").lower() == "true"
    run(args.date, args.limit, full_refresh)


if __name__ == "__main__":
    main()
