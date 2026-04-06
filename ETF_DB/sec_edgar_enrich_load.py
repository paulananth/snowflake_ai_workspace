# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "snowflake-connector-python"]
# ///
"""
SEC EDGAR Bulk Enrichment Loader
---------------------------------
Fetches EDGAR data for all US equities in ETF_DB.LOCAL_COPY.SECURITIES
and writes results back to the same table.

Scope   : asset_class = 'Equity' AND (country_of_exchange = 'US' OR country_of_exchange IS NULL)
Fields  : edgar_cik, edgar_name, sic_code, sic_description, state_of_inc, ein,
          entity_type, listed_exchanges, filer_category, active_flag,
          inactive_reason, shares_outstanding, shares_as_of_date, edgar_enriched_at

Safety  : Checkpoints every CHECKPOINT_EVERY tickers to a local JSON file.
          Resume automatically if interrupted — already-processed tickers are skipped.

Run     : uv run ETF_DB/sec_edgar_enrich_load.py
"""

import json
import pathlib
import re
import time
import tomllib
from datetime import date, datetime, timedelta

import requests
import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
USER_AGENT        = "ETF-Research-Tool/1.0 (contact@example.com)"
TICKERS_URL       = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL   = "https://data.sec.gov/submissions/CIK{cik}.json"
CONCEPT_URLS      = [
    "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/dei/EntityCommonStockSharesOutstanding.json",
    "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/CommonStockSharesOutstanding.json",
]
DELAY             = 0.15          # seconds between HTTP requests (~6-7/sec, under 10 limit)
BATCH_SIZE        = 50            # Snowflake UPDATE rows per executemany call
CHECKPOINT_EVERY  = 100           # save progress to disk every N tickers
LOG_EVERY         = 50            # print progress line every N tickers
CHECKPOINT_FILE   = pathlib.Path("ETF_DB/edgar_enrich_checkpoint.json")

DEREGISTRATION_FORMS = {"15", "15-12G", "15-12B", "15F-12G", "15F-12B"}
SHARES_FORMS         = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
STALE_YEARS          = 2

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def edgar_get(url: str, session: requests.Session) -> dict | None:
    time.sleep(DELAY)
    try:
        r = session.get(url, timeout=20)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        print(f"  [HTTP ERR] {url}: {exc}")
        return None


def build_cik_map(session: requests.Session) -> dict[str, str]:
    print("Loading company_tickers.json ...")
    data = edgar_get(TICKERS_URL, session)
    if not data:
        raise RuntimeError("Failed to fetch company_tickers.json")
    cik_map: dict[str, str] = {}
    for entry in data.values():
        ticker = entry["ticker"].upper().strip()
        cik_map[ticker] = str(entry["cik_str"]).zfill(10)
    print(f"  {len(cik_map):,} ticker->CIK mappings loaded")
    return cik_map


def resolve_cik(ticker: str, cik_map: dict[str, str]) -> str | None:
    for variant in [
        ticker,
        ticker.replace(" ", "-"),
        ticker.replace(".", "-"),
        ticker.replace(" ", ""),
        ticker.replace(".", ""),
        ticker.replace(" ", "."),
    ]:
        cik = cik_map.get(variant.upper())
        if cik:
            return cik
    return None


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " / ", text or "").strip(" /") or None


# ---------------------------------------------------------------------------
# Active flag
# ---------------------------------------------------------------------------

def determine_active(cik_found: bool, submissions: dict | None) -> tuple[bool, str]:
    if not cik_found:
        return False, "not_in_edgar_tickers"
    if submissions is None:
        return True, "submissions_fetch_error"

    recent = submissions.get("filings", {}).get("recent", {})
    forms  = recent.get("form", [])
    dates  = recent.get("filingDate", [])

    for form in forms:
        if form in DEREGISTRATION_FORMS:
            return False, f"deregistration_{form}"

    cutoff = (datetime.today() - timedelta(days=365 * STALE_YEARS)).date()
    has_recent = any(
        f in ("10-K", "10-Q", "10-K/A", "10-Q/A") and d and date.fromisoformat(d) >= cutoff
        for f, d in zip(forms, dates)
    )
    if not has_recent:
        return False, "no_recent_10k_10q"

    return True, "active"


# ---------------------------------------------------------------------------
# Shares outstanding
# ---------------------------------------------------------------------------

def _class_index_from_ticker(ticker: str) -> int | None:
    suffix = re.split(r"[ .\-]", ticker)[-1].upper()
    if suffix == ticker.upper():
        return None
    return {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4,
            "1": 0, "2": 1, "3": 2, "4": 3}.get(suffix)


def fetch_shares(cik: str, ticker: str, session: requests.Session) -> tuple[int | None, str | None]:
    """Returns (shares_outstanding, shares_as_of_date). Both None if unavailable."""
    for url_tpl in CONCEPT_URLS:
        data = edgar_get(url_tpl.format(cik=cik), session)
        if not data:
            continue
        points = [
            p for p in data.get("units", {}).get("shares", [])
            if p.get("form") in SHARES_FORMS and p.get("val") and p.get("filed")
        ]
        if not points:
            continue

        points.sort(key=lambda p: p["filed"], reverse=True)
        latest_filed = points[0]["filed"]
        latest_form  = points[0]["form"]
        same_filing  = [p for p in points if p["filed"] == latest_filed and p["form"] == latest_form]

        if len(same_filing) == 1:
            return same_filing[0]["val"], same_filing[0].get("end")

        # Multi-class: try to map via ticker suffix
        same_filing.sort(key=lambda p: p["val"])
        idx = _class_index_from_ticker(ticker)
        if idx is not None and idx < len(same_filing):
            return same_filing[idx]["val"], same_filing[idx].get("end")

        # Can't resolve — try next concept URL before giving up
        continue

    return None, None


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def snowflake_conn() -> snowflake.connector.SnowflakeConnection:
    config_path = pathlib.Path.home() / ".snowflake" / "config.toml"
    with open(config_path, "rb") as f:
        cfg = tomllib.load(f)["connections"]["myfirstsnow"]
    return snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg.get("role", "ACCOUNTADMIN"), warehouse=cfg.get("warehouse", "cortex_analyst_wh"),
    )


def load_target_tickers(conn) -> list[str]:
    """All US equities not yet enriched (EDGAR_ENRICHED_AT IS NULL)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT constituent_ticker
        FROM ETF_DB.LOCAL_COPY.SECURITIES
        WHERE asset_class = 'Equity'
          AND (country_of_exchange = 'US' OR country_of_exchange IS NULL)
          AND edgar_enriched_at IS NULL
        ORDER BY constituent_ticker
    """)
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    return tickers


def flush_batch(conn, batch: list[tuple]) -> None:
    if not batch:
        return
    cur = conn.cursor()
    cur.executemany("""
        UPDATE ETF_DB.LOCAL_COPY.SECURITIES SET
            EDGAR_CIK          = %s,
            EDGAR_NAME         = %s,
            SIC_CODE           = %s,
            SIC_DESCRIPTION    = %s,
            STATE_OF_INC       = %s,
            EIN                = %s,
            ENTITY_TYPE        = %s,
            LISTED_EXCHANGES   = %s,
            FILER_CATEGORY     = %s,
            ACTIVE_FLAG        = %s,
            INACTIVE_REASON    = %s,
            SHARES_OUTSTANDING = %s,
            SHARES_AS_OF_DATE  = %s,
            EDGAR_ENRICHED_AT  = CURRENT_TIMESTAMP()
        WHERE CONSTITUENT_TICKER = %s
    """, batch)
    cur.close()


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())
        done = set(data.get("done", []))
        print(f"  Resuming from checkpoint: {len(done):,} tickers already processed")
        return done
    return set()


def save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_FILE.write_text(json.dumps({"done": list(done)}, indent=2))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.headers["Accept"]     = "application/json"

    cik_map = build_cik_map(session)

    print("Connecting to Snowflake ...")
    conn = snowflake_conn()
    tickers = load_target_tickers(conn)
    print(f"  {len(tickers):,} US equities to enrich\n")

    done      = load_checkpoint()
    pending   = [t for t in tickers if t not in done]
    print(f"  {len(pending):,} remaining after checkpoint\n")

    batch: list[tuple] = []
    counts = {"ok": 0, "no_cik": 0, "active": 0, "inactive": 0, "shares": 0, "errors": 0}
    start  = datetime.now()

    for i, ticker in enumerate(pending, 1):
        try:
            cik = resolve_cik(ticker, cik_map)

            submissions = None
            edgar_name = sic = sic_desc = state = ein = entity_type = exchanges = category = None

            if cik:
                submissions = edgar_get(SUBMISSIONS_URL.format(cik=cik), session)
                if submissions:
                    exch_raw      = submissions.get("exchanges") or []
                    exchanges     = "|".join(dict.fromkeys(e for e in exch_raw if e)) or None
                    edgar_name    = submissions.get("name")
                    sic           = submissions.get("sic")
                    sic_desc      = submissions.get("sicDescription")
                    state         = submissions.get("stateOfIncorporation")
                    ein           = submissions.get("ein")
                    entity_type   = submissions.get("entityType")
                    category      = strip_html(submissions.get("category") or "")

            active, reason = determine_active(cik is not None, submissions)

            shares, as_of = fetch_shares(cik, ticker, session) if cik else (None, None)

            batch.append((
                cik.lstrip("0") if cik else None,
                edgar_name, sic, sic_desc, state, ein, entity_type,
                exchanges, category,
                active, reason if not active else None,
                shares, as_of,
                ticker,
            ))

            # Counters
            counts["ok"]       += 1
            counts["active"]   += int(active)
            counts["inactive"] += int(not active)
            counts["shares"]   += int(shares is not None)
            if not cik:
                counts["no_cik"] += 1

        except Exception as exc:
            print(f"  [ERR] {ticker}: {exc}")
            counts["errors"] += 1
            # Still write a minimal row so we don't retry endlessly
            batch.append((None,)*13 + (ticker,))

        done.add(ticker)

        # Flush to Snowflake
        if len(batch) >= BATCH_SIZE:
            flush_batch(conn, batch)
            batch.clear()

        # Checkpoint to disk
        if i % CHECKPOINT_EVERY == 0:
            save_checkpoint(done)

        # Progress log
        if i % LOG_EVERY == 0 or i == len(pending):
            elapsed  = (datetime.now() - start).total_seconds()
            rate     = i / elapsed if elapsed > 0 else 0
            eta_sec  = (len(pending) - i) / rate if rate > 0 else 0
            eta_str  = f"{int(eta_sec//60)}m{int(eta_sec%60):02d}s"
            print(
                f"  [{i:>5}/{len(pending)}] "
                f"ok={counts['ok']} active={counts['active']} "
                f"inactive={counts['inactive']} shares={counts['shares']} "
                f"no_cik={counts['no_cik']} err={counts['errors']} "
                f"| {rate:.1f}/s ETA {eta_str}"
            )

    # Final flush + checkpoint
    flush_batch(conn, batch)
    save_checkpoint(done)
    conn.close()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*65}")
    print(f"DONE in {int(elapsed//60)}m{int(elapsed%60):02d}s")
    print(f"  Total processed : {counts['ok']:,}")
    print(f"  Active          : {counts['active']:,}")
    print(f"  Inactive        : {counts['inactive']:,}")
    print(f"  No CIK          : {counts['no_cik']:,}")
    print(f"  With shares     : {counts['shares']:,}")
    print(f"  Errors          : {counts['errors']:,}")
    print(f"\nCheckpoint saved to: {CHECKPOINT_FILE}")
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()   # clean up on successful completion
        print("Checkpoint file removed (clean run).")


if __name__ == "__main__":
    main()
