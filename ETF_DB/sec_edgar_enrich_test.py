# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "snowflake-connector-python"]
# ///
"""
SEC EDGAR Enrichment — Extended Verification Test (25 tickers)
--------------------------------------------------------------
Tests active_flag logic and shares_outstanding before the full bulk load.

Covers:
  - Large-cap actives          (AAPL, MSFT, NVDA, JPM, T)
  - Small-cap actives          (AAME, A, AA, GE, BRK B)
  - Known delisted/acquired    (AADI, TWTR, ATVI, VMW, XLNX)
  - Multi-class share tickers  (GOOG, GOOGL, META, BRK B)
  - Edge cases                 (BF.B -> BF-B, MOG.A)

Run:  uv run ETF_DB/sec_edgar_enrich_test.py
Then: spot-check the SHARES VERIFICATION TABLE against Yahoo Finance or IR pages.
"""

import os
import pathlib
import re
import time
import tomllib
from datetime import date, datetime, timedelta

import requests
import snowflake.connector

# ---------------------------------------------------------------------------
USER_AGENT      = "ETF-Research-Tool/1.0 (contact@example.com)"
TICKERS_URL     = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
CONCEPT_URLS    = [
    # Primary: DEI concept — most filers tag this
    "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/dei/EntityCommonStockSharesOutstanding.json",
    # Fallback: US-GAAP concept — catches multi-class filers (Alphabet, etc.)
    "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/CommonStockSharesOutstanding.json",
]
DELAY           = 0.15   # seconds between EDGAR requests (~6-7 req/sec, under 10 limit)

DEREGISTRATION_FORMS = {"15", "15-12G", "15-12B", "15F-12G", "15F-12B"}
SHARES_FORMS         = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
STALE_YEARS          = 2    # no annual/quarterly filing in this many years = stale

# 25 test tickers (all should be in SECURITIES table as US equities)
PILOT_TICKERS = [
    # Large-cap active
    "AAPL",   # Apple — single class
    "MSFT",   # Microsoft
    "NVDA",   # Nvidia
    "JPM",    # JPMorgan
    "T",      # AT&T
    # Small-cap active
    "GE",     # GE Aerospace (post-split)
    "AA",     # Alcoa
    "A",      # Agilent
    "AAME",   # Atlantic American (micro-cap)
    "BRK B",  # Berkshire B — space in ticker, space->hyphen variant
    # Multi-class (shares should SUM Class A + Class B or match total)
    "GOOG",   # Alphabet Class C
    "GOOGL",  # Alphabet Class A
    "META",   # Meta (single class but large float)
    # Known delisted / acquired
    "AADI",   # Nuvation Bio acquisition 2023
    "TWTR",   # Twitter -> X, delisted 2022
    "ATVI",   # Activision -> Microsoft, delisted 2023
    "VMW",    # VMware -> Broadcom, delisted 2023
    "XLNX",   # Xilinx -> AMD, delisted 2022
    # Space/dot ticker variants
    "BF.B",   # Brown-Forman B (dot separator in our data)
    "MOG.A",  # Moog Inc. Class A
    # Additional spots for coverage
    "WMT",    # Walmart
    "JNJ",    # Johnson & Johnson
    "BAC",    # Bank of America
    "XOM",    # ExxonMobil
    "AMZN",   # Amazon
]


# ---------------------------------------------------------------------------
# Helpers
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
        print(f"    [HTTP ERROR] {url}: {exc}")
        return None


def build_cik_map(session: requests.Session) -> dict[str, str]:
    """Returns {TICKER_UPPER: zero-padded-10-digit-CIK}."""
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
    """Try multiple ticker variants to find a CIK."""
    variants = [
        ticker,
        ticker.replace(" ", "-"),   # "BRK B"  -> "BRK-B"
        ticker.replace(".", "-"),   # "BF.B"   -> "BF-B"
        ticker.replace(" ", ""),    # "BRK B"  -> "BRKB"
        ticker.replace(".", ""),    # "BF.B"   -> "BFB"
        ticker.replace(" ", "."),   # "BRK B"  -> "BRK.B"
    ]
    for v in variants:
        cik = cik_map.get(v.upper())
        if cik:
            return cik
    return None


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " / ", text or "").strip(" /")


def determine_active_flag(
    cik_found: bool,
    submissions: dict | None,
) -> tuple[bool, str]:
    """
    Returns (active_flag: bool, reason: str).
    reason is a short code for logging.
    """
    if not cik_found:
        return False, "not_in_edgar_tickers"

    if submissions is None:
        # CIK found but submissions fetch failed — treat as unknown, default active
        return True, "submissions_fetch_error"

    recent = submissions.get("filings", {}).get("recent", {})
    forms  = recent.get("form", [])
    dates  = recent.get("filingDate", [])

    # Check for explicit deregistration forms
    for form in forms:
        if form in DEREGISTRATION_FORMS:
            return False, f"deregistration_form_{form}"

    # Check for stale filer: no 10-K or 10-Q in last STALE_YEARS years
    cutoff = (datetime.today() - timedelta(days=365 * STALE_YEARS)).date()
    recent_annual_quarterly = [
        d for f, d in zip(forms, dates)
        if f in ("10-K", "10-Q", "10-K/A", "10-Q/A") and d and date.fromisoformat(d) >= cutoff
    ]
    if not recent_annual_quarterly:
        return False, "no_recent_10k_10q"

    return True, "active"


def _class_index_from_ticker(ticker: str) -> int | None:
    """
    Extract a share-class index (0-based) from the ticker suffix.

    Rules:
      - Split on the last space, dot, or hyphen: "BRK B" -> "B", "BF.B" -> "B", "MOG.A" -> "A"
      - Letter A/1 -> index 0, B/2 -> index 1, C/3 -> index 2, D/4 -> index 3
      - Rows are sorted ascending by val before indexing, so:
          index 0 = fewest shares (typically high-vote / A class)
          index 1 = next tier (B class)  etc.
      - If ticker has no recognisable class suffix -> return None (caller sets NULL).
    """
    # Extract suffix after the last separator
    suffix = re.split(r"[ .\-]", ticker)[-1].upper()
    if suffix == ticker.upper():
        # No separator found — no class info extractable
        return None
    letter_map = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4,
                  "1": 0, "2": 1, "3": 2, "4": 3}
    return letter_map.get(suffix)  # None if suffix not in map


def fetch_shares_outstanding(
    cik: str,
    session: requests.Session,
    ticker: str = "",
) -> tuple[int | None, str | None, str | None]:
    """
    Returns (shares: int|None, as_of_date: str|None, source_desc: str|None).
    Tries DEI concept first, then us-gaap fallback.
    Handles multi-class shares by summing same-filing rows.
    """
    for url_template in CONCEPT_URLS:
        url  = url_template.format(cik=cik)
        data = edgar_get(url, session)
        if not data:
            continue

        points = data.get("units", {}).get("shares", [])
        if not points:
            continue

        # Filter to substantive annual/quarterly filings only
        filtered = [
            p for p in points
            if p.get("form") in SHARES_FORMS and p.get("val") and p.get("filed")
        ]
        if not filtered:
            continue

        # Sort by filed date descending, take most recent filing
        filtered.sort(key=lambda p: p["filed"], reverse=True)
        latest_filed = filtered[0]["filed"]
        latest_form  = filtered[0]["form"]

        # Collect all rows with same filed+form date
        same_filing = [
            p for p in filtered
            if p["filed"] == latest_filed and p["form"] == latest_form
        ]
        concept_tag = "dei" if "dei" in url_template else "us-gaap"

        if len(same_filing) == 1:
            return same_filing[0]["val"], same_filing[0].get("end"), f"{latest_form} filed {latest_filed} [{concept_tag}]"

        # Multiple share classes in one filing — try to match by ticker class suffix.
        # Sort ascending by val so index 0=smallest class (usually Class A / high-vote),
        # index 1=next (Class B), index 2=Class C, etc.
        same_filing.sort(key=lambda p: p["val"])
        class_idx = _class_index_from_ticker(ticker)
        if class_idx is not None and class_idx < len(same_filing):
            row = same_filing[class_idx]
            return row["val"], row.get("end"), f"{latest_form} filed {latest_filed} [class_idx={class_idx}] [{concept_tag}]"

        # Can't map ticker to a specific class row — return None
        class_counts = len(same_filing)
        return None, None, f"multi_class_unresolvable ({class_counts} classes, no suffix) [{concept_tag}]"

    return None, None, None


def load_snowflake_tickers(tickers: list[str]) -> set[str]:
    """Return which of the requested tickers exist in SECURITIES."""
    config_path = pathlib.Path.home() / ".snowflake" / "config.toml"
    with open(config_path, "rb") as f:
        toml = tomllib.load(f)
    conn_name = toml.get("default_connection_name") or os.environ.get("SNOWFLAKE_CONNECTION", "snowconn")
    cfg = toml["connections"][conn_name]
    conn = snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg.get("role", "ACCOUNTADMIN"), warehouse=cfg.get("warehouse", "cortex_analyst_wh"),
    )
    ticker_list = ", ".join(f"'{t}'" for t in tickers)
    cur = conn.cursor()
    cur.execute(f"SELECT constituent_ticker FROM ETF_DB.LOCAL_COPY.SECURITIES WHERE constituent_ticker IN ({ticker_list})")
    found = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return found


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.headers["Accept"]     = "application/json"

    cik_map = build_cik_map(session)

    print(f"\nChecking {len(PILOT_TICKERS)} tickers in Snowflake SECURITIES table ...")
    in_securities = load_snowflake_tickers(PILOT_TICKERS)
    print(f"  {len(in_securities)} found in SECURITIES\n")

    results = []

    for ticker in PILOT_TICKERS:
        print(f"{'=' * 65}")
        print(f"TICKER: {ticker}")

        if ticker not in in_securities:
            print(f"  [WARN] Not in SECURITIES table (may be stored differently)")

        cik = resolve_cik(ticker, cik_map)
        cik_found = cik is not None
        print(f"  CIK found : {cik.lstrip('0') if cik else 'NO MATCH'}")

        submissions = None
        if cik:
            submissions = edgar_get(SUBMISSIONS_URL.format(cik=cik), session)

        active, reason = determine_active_flag(cik_found, submissions)
        print(f"  ACTIVE    : {active}  (reason: {reason})")

        # EDGAR metadata (only if submissions fetched)
        edgar_name = sic = sic_desc = state = exchanges = category = None
        if submissions:
            exchanges_raw = submissions.get("exchanges") or []
            exchanges_deduped = list(dict.fromkeys(e for e in exchanges_raw if e))
            edgar_name = submissions.get("name")
            sic        = submissions.get("sic")
            sic_desc   = submissions.get("sicDescription")
            state      = submissions.get("stateOfIncorporation")
            exchanges  = "|".join(exchanges_deduped) if exchanges_deduped else None
            category   = strip_html(submissions.get("category") or "")
            print(f"  Name      : {edgar_name}")
            print(f"  SIC       : {sic} - {sic_desc}")
            print(f"  State     : {state}  |  Exchanges: {exchanges}")
            print(f"  Category  : {category}")

        # Shares outstanding
        shares, as_of, source = None, None, None
        if cik and active:
            shares, as_of, source = fetch_shares_outstanding(cik, session, ticker)
        elif cik and not active:
            # Still try — delisted companies have historical data
            shares, as_of, source = fetch_shares_outstanding(cik, session, ticker)

        if shares is not None:
            print(f"  SHARES    : {shares:,}  as of {as_of}  [{source}]")
        else:
            print(f"  SHARES    : None (no XBRL concept data)")

        results.append({
            "ticker":        ticker,
            "in_securities": ticker in in_securities,
            "cik":           cik.lstrip("0") if cik else None,
            "active":        active,
            "reason":        reason,
            "edgar_name":    edgar_name,
            "sic":           sic,
            "sic_desc":      sic_desc,
            "state":         state,
            "exchanges":     exchanges,
            "shares":        shares,
            "as_of":         as_of,
            "source":        source,
        })

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 65}")
    print(f"SUMMARY  ({len(PILOT_TICKERS)} tickers)")
    print(f"{'=' * 65}")
    ok_active   = [r for r in results if r["active"]]
    ok_inactive = [r for r in results if not r["active"]]
    no_cik      = [r for r in results if r["cik"] is None]
    has_shares  = [r for r in results if r["shares"] is not None]

    print(f"  Active        : {len(ok_active)}")
    print(f"  Inactive      : {len(ok_inactive)}")
    print(f"  No CIK        : {len(no_cik)} -> {[r['ticker'] for r in no_cik]}")
    print(f"  With shares   : {len(has_shares)}")

    reasons = {}
    for r in results:
        reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
    print(f"\n  Active-flag reasons:")
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason:<35} {count}")

    # -----------------------------------------------------------------------
    # Verification table for manual spot-check
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 65}")
    print("SHARES VERIFICATION TABLE")
    print("Spot-check these against Yahoo Finance (Statistics page) or IR site")
    print(f"{'=' * 65}")
    hdr = f"{'Ticker':<10} {'Shares Outstanding':>22} {'As-Of Date':<14} {'Source Form'}"
    print(hdr)
    print("-" * 80)
    for r in results:
        if r["shares"] is not None:
            shares_fmt = f"{r['shares']:,}"
            print(f"{r['ticker']:<10} {shares_fmt:>22} {r['as_of'] or '':14} {r['source'] or ''}")
        else:
            print(f"{r['ticker']:<10} {'N/A':>22} {'':14} no XBRL data")

    print(f"\n{'=' * 65}")
    print("INACTIVE SECURITIES")
    print(f"{'=' * 65}")
    inactive_hdr = f"{'Ticker':<10} {'Reason':<35} {'CIK'}"
    print(inactive_hdr)
    print("-" * 60)
    for r in ok_inactive:
        print(f"{r['ticker']:<10} {r['reason']:<35} {r['cik'] or 'None'}")


if __name__ == "__main__":
    main()
