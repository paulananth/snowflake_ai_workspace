# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "snowflake-connector-python"]
# ///
"""
SEC EDGAR enrichment — PILOT TEST (10 securities)
--------------------------------------------------
Tests the full pipeline before running the bulk load:
  1. Load company_tickers.json from EDGAR -> build ticker->CIK map
  2. Pull 10 US equities from Snowflake SECURITIES table
  3. For each ticker: fetch submissions JSON, extract enrichment fields
  4. Print results and flag any gaps

Run: uv run ETF_DB/sec_edgar_test.py
"""

import json
import pathlib
import time
import tomllib
from pprint import pprint

import requests
import snowflake.connector

# -- Config ------------------------------------------------------------------
USER_AGENT = "ETF-Research-Tool/1.0 (contact@example.com)"  # SEC requires this
EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
RATE_LIMIT_DELAY = 0.12   # 100ms min between requests -> ~8 req/sec (under 10 limit)

# Test with a hand-picked mix: large caps, small caps, ADRs, edge cases
PILOT_TICKERS = [
    "AAPL",   # Apple — large cap, should be perfect
    "MSFT",   # Microsoft
    "NVDA",   # Nvidia
    "JPM",    # JPMorgan
    "AA",     # Alcoa — post-split edge case
    "BRK B",  # Berkshire B — space in ticker (edge case)
    "A",      # Agilent — single-char ticker
    "AADI",   # Small cap
    "AAME",   # Very small cap
    "T",      # AT&T — bond-heavy issuer with debt tickers in the data
]

# Fields to extract from submissions JSON
EXTRACT_FIELDS = [
    "cik",
    "name",           # official SEC company name
    "sic",            # 4-digit SIC code
    "sicDescription", # SIC industry description
    "stateOfIncorporation",
    "ein",
    "entityType",
    "exchanges",      # list of exchanges
    "phone",
    "category",       # filer category (Large accelerated filer, etc.)
]


def load_snowflake_conn() -> snowflake.connector.SnowflakeConnection:
    config_path = pathlib.Path.home() / ".snowflake" / "config.toml"
    with open(config_path, "rb") as f:
        cfg = tomllib.load(f)["connections"]["snowconn"]
    return snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg.get("role", "ACCOUNTADMIN"), warehouse=cfg.get("warehouse", "cortex_analyst_wh"),
    )


def fetch_json(url: str, session: requests.Session) -> dict | None:
    """Fetch JSON from EDGAR with rate-limit-safe delay and error handling."""
    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"    [ERROR] {url}: {e}")
        return None


def build_cik_map(session: requests.Session) -> dict[str, str]:
    """Download company_tickers.json -> {TICKER: zero-padded CIK string}."""
    print("Fetching company_tickers.json from EDGAR…")
    data = fetch_json(EDGAR_TICKERS_URL, session)
    if not data:
        raise RuntimeError("Failed to load company_tickers.json")
    # Structure: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},  ...}
    cik_map = {}
    for entry in data.values():
        ticker = entry["ticker"].upper().strip()
        cik_padded = str(entry["cik_str"]).zfill(10)
        cik_map[ticker] = cik_padded
    print(f"  Loaded {len(cik_map):,} ticker->CIK mappings")
    return cik_map


def fetch_company_info(cik: str, session: requests.Session) -> dict | None:
    """Fetch submissions JSON for a CIK and extract enrichment fields."""
    url = EDGAR_SUBMISSIONS_URL.format(cik=cik)
    data = fetch_json(url, session)
    if not data:
        return None

    # Deduplicate exchanges (some tickers show NYSE|NYSE|NYSE for multiple share classes)
    exchanges_raw = data.get("exchanges", [])
    exchanges_deduped = list(dict.fromkeys(e for e in exchanges_raw if e))

    # Strip HTML from category field (e.g. "Large accelerated filer<br>Smaller reporting company")
    import re as _re
    category_raw = data.get("category") or ""
    category_clean = _re.sub(r"<[^>]+>", " / ", category_raw).strip(" /")

    return {
        "edgar_cik":              cik.lstrip("0"),
        "edgar_name":             data.get("name"),
        "sic_code":               data.get("sic"),
        "sic_description":        data.get("sicDescription"),
        "state_of_incorporation": data.get("stateOfIncorporation"),
        "ein":                    data.get("ein"),
        "entity_type":            data.get("entityType"),
        "exchanges":              "|".join(exchanges_deduped) if exchanges_deduped else None,
        "phone":                  data.get("phone"),
        "category":               category_clean or None,
    }


def get_securities_from_snowflake(conn, tickers: list[str]) -> dict[str, dict]:
    """Pull the pilot tickers from the SECURITIES table."""
    ticker_list = ", ".join(f"'{t}'" for t in tickers)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT constituent_ticker, constituent_name, security_type,
               country_of_exchange, cusip, isin
        FROM ETF_DB.LOCAL_COPY.SECURITIES
        WHERE constituent_ticker IN ({ticker_list})
    """)
    rows = {}
    for row in cur.fetchall():
        rows[row[0]] = {
            "constituent_ticker":  row[0],
            "constituent_name":    row[1],
            "security_type":       row[2],
            "country_of_exchange": row[3],
            "cusip":               row[4],
            "isin":                row[5],
        }
    cur.close()
    return rows


def main():
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.headers["Accept"] = "application/json"

    # 1. Build ticker->CIK map
    cik_map = build_cik_map(session)

    # 2. Load pilot tickers from Snowflake
    print(f"\nConnecting to Snowflake…")
    conn = load_snowflake_conn()
    sf_rows = get_securities_from_snowflake(conn, PILOT_TICKERS)
    conn.close()
    print(f"  Found {len(sf_rows)} of {len(PILOT_TICKERS)} pilot tickers in SECURITIES\n")

    # 3. For each pilot ticker, fetch EDGAR enrichment
    results = []
    not_in_edgar = []
    not_in_securities = []

    for ticker in PILOT_TICKERS:
        ticker_clean = ticker.replace(" ", "")  # "BRK B" -> "BRKB" for EDGAR lookup

        print(f"{'-'*60}")
        print(f"Ticker: {ticker}")

        if ticker not in sf_rows and ticker_clean not in sf_rows:
            print(f"  WARN Not found in SECURITIES table")
            not_in_securities.append(ticker)

        # Try multiple ticker variants: exact, space->hyphen (BRK B->BRK-B), space removed, space->dot
        cik = (cik_map.get(ticker)
               or cik_map.get(ticker.replace(" ", "-"))
               or cik_map.get(ticker_clean)
               or cik_map.get(ticker.replace(" ", ".")))
        if not cik:
            print(f"  FAIL No CIK found in EDGAR company_tickers.json")
            not_in_edgar.append(ticker)
            results.append({"ticker": ticker, "status": "NO_CIK"})
            continue

        print(f"  CIK: {cik.lstrip('0')}")
        info = fetch_company_info(cik, session)
        if not info:
            print(f"  FAIL EDGAR submissions fetch failed (404 or error)")
            results.append({"ticker": ticker, "status": "FETCH_ERROR", "cik": cik})
            continue

        result = {"ticker": ticker, "status": "OK", **info}
        results.append(result)

        # Print extracted fields
        print(f"  Name:          {info['edgar_name']}")
        print(f"  SIC:           {info['sic_code']} — {info['sic_description']}")
        print(f"  State Incorp.: {info['state_of_incorporation']}")
        print(f"  Entity Type:   {info['entity_type']}")
        print(f"  Category:      {info['category']}")
        print(f"  Exchanges:     {info['exchanges']}")
        print(f"  EIN:           {info['ein']}")
        print(f"  Phone:         {info['phone']}")

    # 4. Summary
    print(f"\n{'='*60}")
    print(f"PILOT SUMMARY ({len(PILOT_TICKERS)} tickers tested)")
    print(f"{'='*60}")
    ok      = [r for r in results if r["status"] == "OK"]
    no_cik  = [r for r in results if r["status"] == "NO_CIK"]
    errors  = [r for r in results if r["status"] == "FETCH_ERROR"]
    print(f"  OK Successfully enriched: {len(ok)}")
    print(f"  FAIL No CIK in EDGAR map:   {len(no_cik)} -> {[r['ticker'] for r in no_cik]}")
    print(f"  FAIL Fetch errors:          {len(errors)} -> {[r['ticker'] for r in errors]}")
    print(f"  FAIL Not in SECURITIES:     {len(not_in_securities)} -> {not_in_securities}")

    print(f"\nFields coverage for {len(ok)} successful lookups:")
    for field in ["sic_code", "sic_description", "state_of_incorporation",
                  "ein", "entity_type", "exchanges", "category"]:
        filled = sum(1 for r in ok if r.get(field))
        print(f"  {field:<28} {filled}/{len(ok)}")

    print("\nRaw results:")
    pprint(results, width=100)


if __name__ == "__main__":
    main()
