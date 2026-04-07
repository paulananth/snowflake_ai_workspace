"""
Phase 1 — Standalone SEC EDGAR loader test.

No cloud dependencies (no Azure, no Snowflake).
Fetches data for 10 well-known companies and saves Parquet files locally
to ./output/bronze/ for inspection.

Usage:
    export SEC_USER_AGENT="MyOrg SEC-Test admin@myorg.com"
    python scripts/ingest/test_sec_loader.py

Verify output:
    python -c "
    import duckdb, pathlib
    c = duckdb.connect()
    for p in pathlib.Path('output/bronze').rglob('*.parquet'):
        n = c.execute(f\"SELECT COUNT(*) FROM read_parquet('{p}')\").fetchone()[0]
        print(f'{p}: {n} rows')
    "
"""
import json
import os
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_HALF_UP
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

# _http raises KeyError if SEC_USER_AGENT is not set -- intentional fail-fast
from _http import edgar_get
from _rate_limiter import RateLimiter

# ---------------------------------------------------------------------------
# Pilot CIKs — 10 large, well-known US equities (zero-padded to 10 digits)
# ---------------------------------------------------------------------------
PILOT_CIKS = [
    ("AAPL",  "0000320193"),
    ("MSFT",  "0000789019"),
    ("AMZN",  "0001018724"),
    ("GOOG",  "0001652044"),
    ("META",  "0001326801"),
    ("TSLA",  "0001318605"),
    ("JPM",   "0000019617"),
    ("JNJ",   "0000200406"),
    ("XOM",   "0000034088"),
    ("BRK-B", "0001067983"),
]

# XBRL concept names to extract from companyfacts (us-gaap namespace first,
# falls back to ifrs-full if not present)
XBRL_CONCEPTS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "LongTermDebt",
    "CashAndCashEquivalentsAtCarryingValue",
    "NetCashProvidedByUsedInOperatingActivities",
    "EarningsPerShareBasic",
    "EarningsPerShareDiluted",
]

ANNUAL_DAYS_MIN = 355
ANNUAL_DAYS_MAX = 375

OUTPUT_DIR = pathlib.Path("output/bronze")
INGEST_DATE = date.today().isoformat()

_limiter = RateLimiter(rps=8.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch(url: str) -> dict | None:
    """Rate-limited edgar_get."""
    _limiter.acquire()
    return edgar_get(url)


def _save_parquet(records: list[dict], schema: pa.Schema, path: pathlib.Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, str(path), compression="snappy")
    return len(records)


def _strip_cik(cik10: str) -> str:
    """Remove leading zeros for display only."""
    return str(int(cik10))


# ---------------------------------------------------------------------------
# 1. Ticker exchange snapshot
# ---------------------------------------------------------------------------

def ingest_tickers_exchange() -> pathlib.Path:
    print("\n[1/3] Fetching ticker exchange snapshot...")
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    _limiter.acquire()
    data = edgar_get(url)
    if data is None:
        raise RuntimeError("company_tickers_exchange.json returned 404")

    fields = data.get("fields", [])
    rows = data.get("data", [])
    records = [dict(zip(fields, row)) for row in rows]

    schema = pa.schema([
        pa.field("cik",      pa.int64()),
        pa.field("name",     pa.string()),
        pa.field("ticker",   pa.string()),
        pa.field("exchange", pa.string()),
    ])

    out = OUTPUT_DIR / f"company_tickers_exchange/ingestion_date={INGEST_DATE}/data.parquet"
    n = _save_parquet(records, schema, out)
    print(f"  [OK] {n:,} rows -> {out}")
    return out


# ---------------------------------------------------------------------------
# 2. Submissions (company metadata)
# ---------------------------------------------------------------------------

SUBMISSIONS_SCHEMA = pa.schema([
    pa.field("cik",                   pa.string()),
    pa.field("ticker",                pa.string()),
    pa.field("name",                  pa.string()),
    pa.field("sic",                   pa.string()),
    pa.field("sic_description",       pa.string()),
    pa.field("state_of_incorporation",pa.string()),
    pa.field("ein",                   pa.string()),
    pa.field("entity_type",           pa.string()),
    pa.field("exchanges",             pa.string()),   # JSON-encoded list
    pa.field("filer_category",        pa.string()),
    pa.field("active_flag",           pa.bool_()),
    pa.field("inactive_reason",       pa.string()),
    pa.field("ingestion_date",        pa.string()),
])


def _parse_submission(cik10: str, ticker: str, data: dict) -> dict:
    """Extract fields from a CIK submissions JSON payload."""
    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    dates = filings.get("filingDate", [])

    # Deregistration detection
    dereg_forms = {"15", "15-12G", "15-12B"}
    inactive_reason = None
    active_flag = True
    for form in forms:
        if form in dereg_forms:
            active_flag = False
            inactive_reason = f"Filed form {form}"
            break

    # Require recent 10-K or 10-Q within ~2 years (730 days)
    if active_flag:
        from datetime import datetime, timedelta
        cutoff = (datetime.today() - timedelta(days=730)).date()
        annual_forms = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
        recent_filing = False
        for form, dt_str in zip(forms, dates):
            try:
                if form in annual_forms and date.fromisoformat(dt_str) >= cutoff:
                    recent_filing = True
                    break
            except ValueError:
                pass
        if not recent_filing:
            active_flag = False
            inactive_reason = "No recent 10-K/10-Q within 2 years"

    exchanges_raw = data.get("exchanges", [])
    exchanges = list(dict.fromkeys(exchanges_raw))  # deduplicate, preserve order

    return {
        "cik":                    cik10,
        "ticker":                 ticker,
        "name":                   data.get("name", ""),
        "sic":                    data.get("sic", ""),
        "sic_description":        data.get("sicDescription", ""),
        "state_of_incorporation": data.get("stateOfIncorporation", ""),
        "ein":                    data.get("ein", ""),
        "entity_type":            data.get("entityType", ""),
        "exchanges":              json.dumps(exchanges),
        "filer_category":         data.get("category", ""),
        "active_flag":            active_flag,
        "inactive_reason":        inactive_reason,
        "ingestion_date":         INGEST_DATE,
    }


def _fetch_submission(args: tuple) -> dict | None:
    ticker, cik10 = args
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    data = _fetch(url)
    if data is None:
        print(f"  [WARN] 404 for {ticker} (CIK {_strip_cik(cik10)})")
        return None
    return _parse_submission(cik10, ticker, data)


def ingest_submissions() -> pathlib.Path:
    print("\n[2/3] Fetching submissions for pilot CIKs...")
    records = []
    errors = 0

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_fetch_submission, item): item for item in PILOT_CIKS}
        for future in as_completed(futures):
            result = future.result()
            ticker = futures[future][0]
            if result:
                records.append(result)
                print(f"  [OK] {ticker}: {result['name'][:50]}")
            else:
                errors += 1

    out = OUTPUT_DIR / f"submissions/ingestion_date={INGEST_DATE}/batch_0001.parquet"
    n = _save_parquet(records, SUBMISSIONS_SCHEMA, out)
    print(f"  Saved {n} rows (errors: {errors}) -> {out}")
    return out


# ---------------------------------------------------------------------------
# 3. Company facts (XBRL financial data)
# ---------------------------------------------------------------------------

FACTS_SCHEMA = pa.schema([
    pa.field("cik",          pa.string()),
    pa.field("ticker",       pa.string()),
    pa.field("concept",      pa.string()),
    pa.field("label",        pa.string()),
    pa.field("unit",         pa.string()),
    pa.field("end_date",     pa.string()),
    pa.field("value",        pa.decimal128(38, 4)),
    pa.field("accn",         pa.string()),
    pa.field("form",         pa.string()),
    pa.field("filed",        pa.string()),
    pa.field("period_type",  pa.string()),   # "annual" | "quarterly" | "instant"
    pa.field("ingestion_date", pa.string()),
])


def _to_decimal_38_4(value: object) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _period_type(entry: dict) -> str:
    if "start" not in entry:
        return "instant"
    try:
        start = date.fromisoformat(entry["start"])
        end   = date.fromisoformat(entry["end"])
        days  = (end - start).days
        if ANNUAL_DAYS_MIN <= days <= ANNUAL_DAYS_MAX:
            return "annual"
        if 85 <= days <= 100:
            return "quarterly"
        return "other"
    except ValueError:
        return "other"


def _parse_facts(cik10: str, ticker: str, data: dict) -> list[dict]:
    records = []
    facts_root = data.get("facts", {})

    for namespace in ("us-gaap", "ifrs-full"):
        ns_data = facts_root.get(namespace, {})
        for concept in XBRL_CONCEPTS:
            concept_data = ns_data.get(concept)
            if concept_data is None:
                continue
            label = concept_data.get("label", concept)
            units = concept_data.get("units", {})
            for unit_key, entries in units.items():
                for entry in entries:
                    pt = _period_type(entry)
                    if pt not in ("annual", "instant"):
                        continue
                    records.append({
                        "cik":           cik10,
                        "ticker":        ticker,
                        "concept":       concept,
                        "label":         label,
                        "unit":          unit_key,
                        "end_date":      entry.get("end", ""),
                        "value":         _to_decimal_38_4(entry.get("val", 0)),
                        "accn":          entry.get("accn", ""),
                        "form":          entry.get("form", ""),
                        "filed":         entry.get("filed", ""),
                        "period_type":   pt,
                        "ingestion_date": INGEST_DATE,
                    })
        if records:
            break   # found data in us-gaap, skip ifrs-full

    return records


def _fetch_facts(args: tuple) -> list[dict]:
    ticker, cik10 = args
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
    data = _fetch(url)
    if data is None:
        print(f"  [WARN] 404 for {ticker} companyfacts")
        return []
    rows = _parse_facts(cik10, ticker, data)
    print(f"  [OK] {ticker}: {len(rows)} fact rows")
    return rows


def ingest_companyfacts() -> pathlib.Path:
    print("\n[3/3] Fetching XBRL company facts for pilot CIKs...")
    all_records: list[dict] = []

    # Sequential (not parallel) — companyfacts payloads are large (~5-15 MB each)
    # and the rate limiter already enforces 8 req/s; parallelism here adds no benefit.
    for item in PILOT_CIKS:
        rows = _fetch_facts(item)
        all_records.extend(rows)

    out = OUTPUT_DIR / f"companyfacts/ingestion_date={INGEST_DATE}/batch_0001.parquet"
    n = _save_parquet(all_records, FACTS_SCHEMA, out)
    print(f"  Saved {n:,} fact rows -> {out}")
    return out


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def _coverage_report(path: pathlib.Path) -> None:
    try:
        import duckdb
        c = duckdb.connect()
        df = c.execute(f"SELECT * FROM read_parquet('{path}')").df()
        print(f"\n  Coverage for {path.name}:")
        for col in df.columns:
            filled = df[col].notna().sum()
            pct = 100 * filled / max(len(df), 1)
            print(f"    {col:<35} {pct:5.1f}%  ({filled}/{len(df)})")
    except ImportError:
        print("  (install duckdb to see coverage report)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    user_agent = os.environ.get("SEC_USER_AGENT", "")
    if not user_agent:
        print("ERROR: SEC_USER_AGENT env var is not set.")
        print('Set it to: export SEC_USER_AGENT="YourOrg Pipeline you@yourorg.com"')
        sys.exit(1)

    print(f"SEC_USER_AGENT : {user_agent}")
    print(f"Ingest date    : {INGEST_DATE}")
    print(f"Output dir     : {OUTPUT_DIR.resolve()}")
    print(f"Pilot CIKs     : {len(PILOT_CIKS)}")

    tickers_path = ingest_tickers_exchange()
    submissions_path = ingest_submissions()
    facts_path = ingest_companyfacts()

    print("\n" + "=" * 60)
    print("PHASE 1 COMPLETE")
    print("=" * 60)
    for p in [tickers_path, submissions_path, facts_path]:
        size_kb = p.stat().st_size // 1024
        print(f"  {p}  ({size_kb} KB)")

    # Optional coverage report (requires duckdb)
    _coverage_report(submissions_path)


if __name__ == "__main__":
    main()
