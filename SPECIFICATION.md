# SEC EDGAR Security & Financial Data Platform — SPECIFICATION.md

**Platform:** Databricks (cloud-agnostic)  **Architecture:** Medallion (Bronze / Silver / Gold)  **Source:** SEC EDGAR only  **Storage:** Parquet files in object storage + Delta tables

## 1. System Overview

This is a **greenfield** Databricks-based platform for ingesting, storing, and serving SEC EDGAR financial data. There are no existing tables, schemas, or pipelines — everything is built from scratch.

**Goals:**
- Store all raw SEC EDGAR API responses as Parquet files (auditable, reproducible)
- Build a curated security master with a stable surrogate key (`security_id`)
- Parse XBRL financial facts into analytics-ready Delta tables
- Support any cloud (AWS S3, Azure ADLS, GCP GCS) via a single `STORAGE_ROOT` config variable
- Serve as the foundation for a security and financial analysis system

---

## 2. Architecture Overview

```
SEC EDGAR APIs
     │
     ▼
[Python Ingestion Driver]  ← single-node, rate-limited (≤10 req/s)
     │  HTTP → Parquet
     ▼
Object Storage (STORAGE_ROOT)
  /bronze/
    company_tickers_exchange/ingestion_date=YYYY-MM-DD/data.parquet
    submissions/ingestion_date=YYYY-MM-DD/cik={CIK}/data.parquet
    companyfacts/ingestion_date=YYYY-MM-DD/cik={CIK}/data.parquet
     │
     ▼ (Auto Loader / COPY INTO)
[Bronze Layer]  sec_edgar_platform.bronze.*   — raw Delta tables, append-only
     │
     ▼ (PySpark jobs)
[Silver Layer]  sec_edgar_platform.silver.*   — parsed, validated, deduplicated
     │
     ▼ (PySpark jobs)
[Gold Layer]    sec_edgar_platform.gold.*     — analytics-ready aggregates
```

**Key design principles:**
- Bronze is append-only (never updated, full audit trail)
- Silver uses MERGE (idempotent upserts keyed on natural business keys)
- Gold is rebuilt from Silver on each run (no incremental complexity)
- `security_id` is a deterministic 16-char hex hash — stable across re-runs, no sequences needed

---

## 3. Prerequisites

1. **Databricks workspace** with Unity Catalog enabled
2. **Catalog created**: `CREATE CATALOG IF NOT EXISTS sec_edgar_platform;`
3. **Object storage bucket/container** accessible from the workspace (any cloud)
4. **Service principal or personal access token** with WRITE on the storage location
5. **Python 3.11+** (for the ingestion driver script)
6. **Databricks CLI** (`databricks`) installed and configured for job deployment

---

## 4. Configuration

All environment-specific settings live in a single config file `config/settings.py`:

```python
# config/settings.py — edit these before first run

# Root path of object storage. Examples:
#   AWS:   "s3://my-bucket/sec-edgar"
#   Azure: "abfss://container@account.dfs.core.windows.net/sec-edgar"
#   GCP:   "gs://my-bucket/sec-edgar"
STORAGE_ROOT = "s3://my-bucket/sec-edgar"   # CHANGE THIS

CATALOG_NAME = "sec_edgar_platform"         # Unity Catalog catalog name
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

# SEC EDGAR API
SEC_BASE_URL     = "https://data.sec.gov"
SEC_FILES_URL    = "https://www.sec.gov/files"
USER_AGENT       = "MyOrg DataPipeline contact@myorg.com"  # CHANGE THIS — required by SEC
REQUEST_DELAY_S  = 0.15    # seconds between HTTP requests (≤10 req/s per SEC policy)
REQUEST_TIMEOUT  = 30
MAX_RETRIES      = 3

# Exchange filter for active equities
TARGET_EXCHANGES = ["NYSE", "Nasdaq"]
```

The Databricks jobs read these via `%run ./config/settings` or by importing the module.

---

## 5. Unity Catalog Setup

Run once, before anything else:

```sql
-- Run as a workspace admin
CREATE CATALOG IF NOT EXISTS sec_edgar_platform;

CREATE SCHEMA IF NOT EXISTS sec_edgar_platform.bronze;
CREATE SCHEMA IF NOT EXISTS sec_edgar_platform.silver;
CREATE SCHEMA IF NOT EXISTS sec_edgar_platform.gold;

-- Register storage location (update path to match your cloud)
CREATE EXTERNAL LOCATION IF NOT EXISTS sec_edgar_storage
  URL 'abfss://container@account.dfs.core.windows.net/sec-edgar'  -- change this
  WITH (STORAGE CREDENTIAL your_credential_name);                  -- change this

GRANT CREATE TABLE, READ FILES, WRITE FILES
  ON EXTERNAL LOCATION sec_edgar_storage
  TO `data-engineers`;                                             -- change to your group
```

---

## 6. Object Storage Layout (Bronze Parquet Files)

Raw API responses are written as Parquet under `STORAGE_ROOT`. The directory structure is:

```
{STORAGE_ROOT}/
  bronze/
    company_tickers_exchange/
      ingestion_date=2024-01-25/
        data.parquet          ← full snapshot from company_tickers_exchange.json
      ingestion_date=2024-01-26/
        data.parquet
    submissions/
      ingestion_date=2024-01-25/
        cik=0000320193/
          data.parquet        ← one parquet per CIK
        cik=0000789019/
          data.parquet
    companyfacts/
      ingestion_date=2024-01-25/
        cik=0000320193/
          data.parquet
```

**Why Parquet (not JSON):** columnar compression (10–20× smaller), schema-on-read via Auto Loader, Delta Lake can register as external table.

---

## 7. Bronze Layer — Delta Table DDL

Run `scripts/setup/01_create_bronze_tables.sql` in a Databricks notebook or SQL editor.

### 7.1 `bronze.company_tickers_exchange_raw`

One row per company per daily snapshot. Append-only.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.bronze.company_tickers_exchange_raw (
  ingestion_date   DATE         NOT NULL,
  cik              BIGINT       NOT NULL,
  company_name     STRING,
  ticker           STRING,
  exchange         STRING,
  source_file_path STRING,                           -- full path to parquet in object storage
  ingested_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
```

### 7.2 `bronze.submissions_raw`

One row per CIK per daily ingestion. Stores key metadata fields; filing arrays stored as JSON string.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.bronze.submissions_raw (
  ingestion_date          DATE      NOT NULL,
  cik                     BIGINT    NOT NULL,
  entity_name             STRING,
  entity_type             STRING,
  sic                     STRING,
  sic_description         STRING,
  ein                     STRING,
  category                STRING,
  fiscal_year_end         STRING,
  state_of_incorporation  STRING,
  tickers_json            STRING,                   -- JSON array: ["AAPL"]
  exchanges_json          STRING,                   -- JSON array: ["Nasdaq"]
  filings_recent_json     STRING,                   -- full filings.recent object as JSON string
  filings_files_json      STRING,                   -- filings.files pagination array as JSON
  source_file_path        STRING,
  ingested_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (ingestion_date)
CLUSTER BY (cik)
TBLPROPERTIES ('delta.appendOnly' = 'true');
```

### 7.3 `bronze.companyfacts_raw`

One row per CIK per daily ingestion. The full facts payload as a JSON string (2–5 MB each).

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.bronze.companyfacts_raw (
  ingestion_date   DATE      NOT NULL,
  cik              BIGINT    NOT NULL,
  entity_name      STRING,
  facts_json       STRING,                          -- full facts object: {"us-gaap":{...},"dei":{...}}
  source_file_path STRING,
  ingested_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (ingestion_date)
CLUSTER BY (cik)
TBLPROPERTIES ('delta.appendOnly' = 'true');
```

### 7.4 `bronze.ingestion_log`

Audit trail for each pipeline run.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.bronze.ingestion_log (
  run_id           STRING    NOT NULL,              -- UUID, generated per pipeline run
  run_date         DATE      NOT NULL,
  pipeline_name    STRING    NOT NULL,
  status           STRING    NOT NULL,              -- 'running' | 'success' | 'partial' | 'failed'
  companies_total  INT,
  companies_ok     INT,
  companies_err    INT,
  error_summary    STRING,
  started_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  finished_at      TIMESTAMP
)
USING DELTA
CLUSTER BY (run_date);
```

---

## 8. Silver Layer — Delta Table DDL

Run `scripts/setup/02_create_silver_tables.sql`.

### 8.1 `silver.dim_security` — Security Master (Golden Record)

One row per unique security. The `security_id` is the system-wide surrogate key.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.silver.dim_security (
  security_id           STRING    NOT NULL,         -- 16-char hex, SHA-256(lpad(cik,10,'0')+'|'+upper(ticker_class))
  cik                   BIGINT    NOT NULL,
  ticker                STRING    NOT NULL,
  ticker_class          STRING,                     -- NULL = primary class; 'A','B','C' = multi-class
  company_name          STRING,
  exchange              STRING,
  sic                   STRING,
  sic_description       STRING,
  entity_type           STRING,
  state_of_incorporation STRING,
  fiscal_year_end       STRING,
  category              STRING,                     -- 'Large accelerated filer', etc.
  active_flag           BOOLEAN   NOT NULL DEFAULT TRUE,
  inactive_reason       STRING,                     -- NULL | 'deregistered' | 'delisted' | 'stale_filings'
  first_seen_date       DATE,                       -- first ingestion_date this CIK/ticker appeared
  last_seen_date        DATE,                       -- most recent ingestion_date it appeared
  source                STRING    NOT NULL DEFAULT 'sec_edgar',
  created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (security_id, cik)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

**`security_id` generation (Python):**
```python
import hashlib

def make_security_id(cik: int, ticker_class: str | None) -> str:
    tc = (ticker_class or "").upper().strip() or "PRIMARY"
    key = f"{str(cik).zfill(10)}|{tc}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]

# Example: CIK 320193, class None → make_security_id(320193, None) → fixed 16-char hex
```

**`security_id` generation (SQL, for Silver MERGE):**
```sql
left(sha2(concat(lpad(cast(cik as string), 10, '0'), '|', upper(coalesce(ticker_class, 'PRIMARY'))), 256), 16)
```

### 8.2 `silver.security_xref` — Cross-Reference Table

Maps every external identifier to `security_id`. Extensible to future data sources.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.silver.security_xref (
  security_id    STRING    NOT NULL,
  source_system  STRING    NOT NULL,                -- 'sec_edgar' | 'bloomberg' | 'refinitiv' | etc.
  id_type        STRING    NOT NULL,                -- 'CIK' | 'TICKER' | 'ISIN' | 'CUSIP' | 'FIGI'
  id_value       STRING    NOT NULL,
  valid_from     DATE      NOT NULL,
  valid_to       DATE,                              -- NULL = currently valid
  created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (security_id, source_system);

### 8.3 `silver.company_ticker_history`

Tracks changes to ticker and exchange over time (detected by diffing daily Bronze snapshots).

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.silver.company_ticker_history (
  security_id    STRING    NOT NULL,
  cik            BIGINT    NOT NULL,
  ticker         STRING    NOT NULL,
  exchange       STRING,
  change_type    STRING    NOT NULL,                -- 'added' | 'removed' | 'exchange_changed' | 'ticker_changed'
  change_date    DATE      NOT NULL,
  old_ticker     STRING,
  old_exchange   STRING,
  detected_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (cik, change_date);
```

### 8.4 `silver.filings_index`

Lightweight catalog of company filings. No document content stored.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.silver.filings_index (
  security_id      STRING    NOT NULL,
  cik              BIGINT    NOT NULL,
  accession_number STRING    NOT NULL,
  form_type        STRING    NOT NULL,
  filed_date       DATE      NOT NULL,
  period_of_report DATE,
  filing_url       STRING,                         -- https://www.sec.gov/Archives/edgar/data/{cik}/{accn_clean}/
  primary_document STRING,
  is_xbrl          BOOLEAN,
  items            STRING,                         -- 8-K items, e.g. '2.01,9.01'
  ingested_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (security_id, filed_date)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

### 8.5 `silver.financial_facts`

One row per company per reporting period per form type. Covers all key XBRL concepts.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.silver.financial_facts (
  security_id          STRING    NOT NULL,
  cik                  BIGINT    NOT NULL,
  period_end           DATE      NOT NULL,
  period_start         DATE,                       -- NULL for instant (balance sheet) concepts
  form_type            STRING    NOT NULL,         -- '10-K' | '10-Q' | '10-K/A' | '10-Q/A'
  filed_date           DATE      NOT NULL,
  fiscal_year          INT,
  fiscal_period        STRING,                     -- 'FY' | 'Q1' | 'Q2' | 'Q3' | 'Q4'
  taxonomy             STRING    NOT NULL,         -- 'us-gaap' | 'ifrs-full'
  -- Income Statement
  revenues             DECIMAL(22,2),
  net_income           DECIMAL(22,2),
  operating_income     DECIMAL(22,2),
  -- Balance Sheet (instant)
  total_assets         DECIMAL(22,2),
  total_liabilities    DECIMAL(22,2),
  stockholders_equity  DECIMAL(22,2),
  long_term_debt       DECIMAL(22,2),
  cash_and_equivalents DECIMAL(22,2),
  shares_outstanding   BIGINT,
  -- Cash Flow
  operating_cash_flow  DECIMAL(22,2),
  -- Per-Share
  eps_basic            DECIMAL(14,4),
  eps_diluted          DECIMAL(14,4),
  ingested_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (security_id, period_end)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

### 8.6 `silver.corporate_actions`

Corporate events detected from filing form types.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.silver.corporate_actions (
  security_id              STRING    NOT NULL,
  cik                      BIGINT    NOT NULL,
  event_type               STRING    NOT NULL,     -- see vocabulary below
  event_date               DATE      NOT NULL,
  effective_date           DATE,
  accession_number         STRING,
  form_type                STRING,
  filed_date               DATE,
  counterparty_cik         BIGINT,
  counterparty_name        STRING,
  split_ratio_numerator    INT,
  split_ratio_denominator  INT,
  description              STRING,
  ingested_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (security_id, event_date);
-- event_type vocabulary: 'merger_acquisition' | 'bankruptcy_filing' | 'stock_split'
--                        | 'reverse_split' | 'deregistration' | 'going_private'
```

---

---

## 9. Gold Layer — Delta Table DDL

Run `scripts/setup/03_create_gold_tables.sql`. Gold tables are fully rebuilt from Silver on each run.

### 9.1 `gold.financial_statements_annual`

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.gold.financial_statements_annual (
  security_id          STRING,
  cik                  BIGINT,
  ticker               STRING,
  company_name         STRING,
  exchange             STRING,
  fiscal_year          INT,
  period_end           DATE,
  filed_date           DATE,
  taxonomy             STRING,
  revenues             DECIMAL(22,2),
  net_income           DECIMAL(22,2),
  operating_income     DECIMAL(22,2),
  total_assets         DECIMAL(22,2),
  total_liabilities    DECIMAL(22,2),
  stockholders_equity  DECIMAL(22,2),
  long_term_debt       DECIMAL(22,2),
  cash_and_equivalents DECIMAL(22,2),
  operating_cash_flow  DECIMAL(22,2),
  eps_basic            DECIMAL(14,4),
  eps_diluted          DECIMAL(14,4),
  -- Derived ratios
  net_margin           DECIMAL(10,6),              -- net_income / revenues
  return_on_equity     DECIMAL(10,6),              -- net_income / stockholders_equity
  debt_to_equity       DECIMAL(10,6),              -- long_term_debt / stockholders_equity
  refreshed_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (ticker, fiscal_year);
```

### 9.2 `gold.financial_statements_quarterly`

Same structure as annual, but for quarterly (10-Q) periods. Most recent 8 quarters.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.gold.financial_statements_quarterly (
  security_id          STRING,
  cik                  BIGINT,
  ticker               STRING,
  company_name         STRING,
  exchange             STRING,
  fiscal_year          INT,
  fiscal_period        STRING,                     -- 'Q1' | 'Q2' | 'Q3' | 'Q4'
  period_end           DATE,
  filed_date           DATE,
  taxonomy             STRING,
  revenues             DECIMAL(22,2),
  net_income           DECIMAL(22,2),
  operating_income     DECIMAL(22,2),
  total_assets         DECIMAL(22,2),
  total_liabilities    DECIMAL(22,2),
  stockholders_equity  DECIMAL(22,2),
  long_term_debt       DECIMAL(22,2),
  cash_and_equivalents DECIMAL(22,2),
  operating_cash_flow  DECIMAL(22,2),
  eps_basic            DECIMAL(14,4),
  eps_diluted          DECIMAL(14,4),
  refreshed_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (ticker, period_end);
```

### 9.3 `gold.company_profile`

One row per security. Combines security master + latest financials.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.gold.company_profile (
  security_id          STRING,
  cik                  BIGINT,
  ticker               STRING,
  company_name         STRING,
  exchange             STRING,
  sic                  STRING,
  sic_description      STRING,
  state_of_incorporation STRING,
  fiscal_year_end      STRING,
  active_flag          BOOLEAN,
  inactive_reason      STRING,
  latest_10k_date      DATE,
  latest_10q_date      DATE,
  latest_revenues      DECIMAL(22,2),
  latest_total_assets  DECIMAL(22,2),
  latest_shares_outstanding BIGINT,
  latest_eps_diluted   DECIMAL(14,4),
  refreshed_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (ticker);
```

### 9.4 `gold.filing_catalog`

All indexed filings with direct URLs, joinable to company_profile.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.gold.filing_catalog (
  security_id      STRING,
  cik              BIGINT,
  ticker           STRING,
  company_name     STRING,
  accession_number STRING,
  form_type        STRING,
  filed_date       DATE,
  period_of_report DATE,
  filing_url       STRING,
  primary_document STRING,
  refreshed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (ticker, filed_date);
```

### 9.5 `gold.corporate_events`

Corporate events enriched with company name and ticker for easy querying.

```sql
CREATE TABLE IF NOT EXISTS sec_edgar_platform.gold.corporate_events (
  security_id     STRING,
  cik             BIGINT,
  ticker          STRING,
  company_name    STRING,
  event_type      STRING,
  event_date      DATE,
  effective_date  DATE,
  form_type       STRING,
  filed_date      DATE,
  description     STRING,
  refreshed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (ticker, event_date);

---

---

## 10. SEC EDGAR API Reference

### Endpoints Used

| API | URL | Frequency |
|---|---|---|
| Ticker/exchange snapshot | `https://www.sec.gov/files/company_tickers_exchange.json` | Daily |
| Company submissions | `https://data.sec.gov/submissions/CIK{cik10}.json` | Daily per CIK |
| XBRL company facts | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json` | Daily per CIK |

`cik10` = CIK zero-padded to 10 digits (e.g. CIK 320193 → `0000320193`)

**Required HTTP headers (per SEC policy):**
```
User-Agent: MyOrg DataPipeline contact@myorg.com
Accept-Encoding: gzip, deflate
```

### `company_tickers_exchange.json` Structure

```json
{
  "fields": ["cik", "name", "ticker", "exchange"],
  "data": [
    [320193, "Apple Inc.", "AAPL", "Nasdaq"],
    [789019, "MICROSOFT CORP", "MSFT", "Nasdaq"]
  ]
}
```

### `submissions/CIK{cik10}.json` Key Fields

```json
{
  "cik": "0000320193",
  "name": "Apple Inc.",
  "entityType": "operating",
  "sic": "3571",
  "sicDescription": "Electronic Computers",
  "tickers": ["AAPL"],
  "exchanges": ["Nasdaq"],
  "ein": "94-2404110",
  "category": "Large accelerated filer",
  "fiscalYearEnd": "0928",
  "stateOfIncorporation": "CA",
  "filings": {
    "recent": {
      "accessionNumber": ["0000320193-23-000064", ...],
      "filingDate":      ["2023-11-02", ...],
      "reportDate":      ["2023-09-30", ...],
      "form":            ["10-K", "10-Q", "8-K", ...],
      "items":           ["", "2.01,9.01", ...],
      "isXBRL":          [1, 1, 0, ...],
      "primaryDocument": ["aapl-20231231.htm", ...]
    },
    "files": [
      { "name": "CIK0000320193-submissions-001.json", "filingCount": 40 }
    ]
  }
}
```

`filings.recent` is parallel arrays (up to 1,000 entries, newest first). `filings.files` = overflow pages for older filings.

### `companyfacts/CIK{cik10}.json` Structure

```json
{
  "cik": 320193,
  "entityName": "Apple Inc.",
  "facts": {
    "dei": {
      "EntityCommonStockSharesOutstanding": {
        "units": { "shares": [ { "end": "2023-09-30", "val": 15634232000,
            "form": "10-K", "filed": "2023-11-02" } ] }
      }
    },
    "us-gaap": {
      "Assets": {
        "units": { "USD": [ { "end": "2023-09-30", "val": 352583000000,
            "form": "10-K", "filed": "2023-11-02" } ] }
      },
      "NetIncomeLoss": {
        "units": { "USD": [ { "start": "2022-09-25", "end": "2023-09-30",
            "val": 96995000000, "fp": "FY", "form": "10-K", "filed": "2023-11-02" } ] }
      }
    }
  }
}
```

**Flow vs. instant:** Flow concepts (income statement, cash flow) have `start` + `end`. Instant concepts (balance sheet) have `end` only.

### XBRL Concept Mapping

| Silver Column | Primary (us-gaap) | Fallback | IFRS |
|---|---|---|---|
| revenues | `Revenues` | `RevenueFromContractWithCustomerExcludingAssessedTax` | `ifrs-full/Revenue` |
| net_income | `NetIncomeLoss` | `ProfitLoss` | `ifrs-full/ProfitLoss` |
| operating_income | `OperatingIncomeLoss` | — | — |
| total_assets | `Assets` | — | `ifrs-full/Assets` |
| total_liabilities | `Liabilities` | — | `ifrs-full/Liabilities` |
| stockholders_equity | `StockholdersEquity` | `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | `ifrs-full/EquityAttributableToOwnersOfParent` |
| long_term_debt | `LongTermDebt` | `LongTermDebtNoncurrent` | `ifrs-full/NoncurrentPortionOfLongtermBorrowings` |
| cash_and_equivalents | `CashAndCashEquivalentsAtCarryingValue` | — | `ifrs-full/CashAndCashEquivalents` |
| operating_cash_flow | `NetCashProvidedByUsedInOperatingActivities` | — | `ifrs-full/CashFlowsFromUsedInOperatingActivities` |
| eps_basic | `EarningsPerShareBasic` | — | `ifrs-full/BasicEarningsLossPerShare` |
| eps_diluted | `EarningsPerShareDiluted` | — | `ifrs-full/DilutedEarningsLossPerShare` |
| shares_outstanding | `dei/EntityCommonStockSharesOutstanding` | `us-gaap/CommonStockSharesOutstanding` | — |

### Period Filtering Rules

```python
def period_days(start: str, end: str) -> int:
    from datetime import date
    return (date.fromisoformat(end) - date.fromisoformat(start)).days

# Annual flow fact:     355 <= period_days(start, end) <= 375
# Quarterly flow fact:   85 <= period_days(start, end) <= 100
# Reject cumulative YTD (~270 days for Q1–Q3)
# Instant fact: no 'start' key at all

# Deduplication: same (cik, period_end, form_type) → keep latest filed_date (handles amendments)
```

### Corporate Action Detection

| event_type | Trigger Form(s) |
|---|---|
| `merger_acquisition` | SC 13E-3, DEFM14A, S-4, 424B3 |
| `bankruptcy_filing` | 8-K with item 1.03 |
| `stock_split` | 8-K item 5.03 |
| `deregistration` | 15, 15-12G, 15-12B |
| `going_private` | SC 13E-3 |

---

## 11. Pipeline Scripts

All scripts live under `scripts/`. They are standalone Python files that run either locally (for the HTTP ingestion driver) or as Databricks notebooks/jobs (for Spark-based Silver/Gold transforms).

### 11.1 `scripts/ingest/01_ingest_tickers_exchange.py` — Daily Tickers Snapshot

Single-node Python. Downloads `company_tickers_exchange.json`, writes Parquet to object storage, then loads Bronze Delta table.

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "pyarrow", "pandas"]
# ///

"""
Usage: python scripts/ingest/01_ingest_tickers_exchange.py [--date YYYY-MM-DD]
Default date = today. Idempotent — skips if parquet file already exists for that date.
"""

import requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from datetime import date
from pathlib import PurePosixPath
import sys, os

sys.path.insert(0, str(Path(__file__).parents[2]))
from config.settings import STORAGE_ROOT, USER_AGENT, REQUEST_DELAY_S, TARGET_EXCHANGES

def run(ingest_date: date):
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    df = pd.DataFrame(payload["data"], columns=payload["fields"])
    df["ingestion_date"] = ingest_date.isoformat()
    df["source_file_path"] = f"{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={ingest_date}/data.parquet"

    out_path = f"{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={ingest_date}/data.parquet"
    # Write to object storage (uses configured cloud credentials from environment)
    pq.write_table(pa.Table.from_pandas(df), out_path)
    print(f"Written {len(df)} rows to {out_path}")

if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    run(d)
```

After writing Parquet, load into Bronze Delta via Spark (run in Databricks notebook):
```python
spark.read.parquet(f"{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={ingest_date}/") \
    .write.format("delta").mode("append") \
    .option("mergeSchema", "false") \
    .saveAsTable("sec_edgar_platform.bronze.company_tickers_exchange_raw")
```

### 11.2 `scripts/ingest/02_ingest_submissions.py` — Daily Submissions per CIK

Single-node Python. Iterates over all CIKs from the latest tickers snapshot, fetches `submissions/CIK{cik}.json`, writes one Parquet file per CIK.

**Rate limiting:** `time.sleep(REQUEST_DELAY_S)` between every request.

**Execution flow:**
```
1. Read today's CIKs from {STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={today}/
2. For each CIK (filtered to TARGET_EXCHANGES):
   a. Check if {STORAGE_ROOT}/bronze/submissions/ingestion_date={today}/cik={cik10}/ exists → skip if yes
   b. GET https://data.sec.gov/submissions/CIK{cik10}.json
   c. If filings.files is non-empty, fetch continuation pages (cap at 5)
   d. Write Parquet: {STORAGE_ROOT}/bronze/submissions/ingestion_date={today}/cik={cik10}/data.parquet
   e. sleep(REQUEST_DELAY_S)
3. Log: N fetched, N skipped, N errors
```

### 11.3 `scripts/ingest/03_ingest_companyfacts.py` — Daily Company Facts (XBRL)

Identical pattern to submissions. Fetches `companyfacts/CIK{cik10}.json`. The facts JSON (~2–5 MB) is stored as a single string column in the Parquet.

**Skip condition:** CIKs not in TARGET_EXCHANGES exchange list; CIKs that returned 404 previously (tracked in a local checkpoint file).

### 11.4 `scripts/bronze_to_silver/01_silver_dim_security.py` — Security Master (Databricks Notebook)

PySpark. Reads latest Bronze `company_tickers_exchange_raw`, computes `security_id`, MERGEs into `silver.dim_security`.

```python
from pyspark.sql import functions as F
from datetime import date

today = date.today().isoformat()

bronze = spark.table("sec_edgar_platform.bronze.company_tickers_exchange_raw") \
    .filter(F.col("ingestion_date") == today) \
    .filter(F.col("exchange").isin(["NYSE", "Nasdaq"]))

silver_candidates = bronze.withColumn(
    "security_id",
    F.left(F.sha2(
        F.concat(F.lpad(F.col("cik").cast("string"), 10, "0"),
                 F.lit("|"),
                 F.upper(F.coalesce(F.lit("PRIMARY"), F.lit("PRIMARY")))),
        256), 16)
).withColumn("active_flag", F.lit(True)) \
 .withColumn("first_seen_date", F.lit(today).cast("date")) \
 .withColumn("last_seen_date", F.lit(today).cast("date"))

# MERGE into silver.dim_security
silver_candidates.createOrReplaceTempView("new_securities")

spark.sql("""
  MERGE INTO sec_edgar_platform.silver.dim_security AS target
  USING new_securities AS source
  ON target.security_id = source.security_id
  WHEN MATCHED THEN UPDATE SET
    target.last_seen_date = source.last_seen_date,
    target.company_name   = source.company_name,
    target.exchange       = source.exchange,
    target.updated_at     = current_timestamp()
  WHEN NOT MATCHED THEN INSERT *
""")
```

### 11.5 `scripts/bronze_to_silver/02_silver_filings_and_facts.py`

PySpark. Reads Bronze `submissions_raw` and `companyfacts_raw`, parses JSON, applies XBRL concept extraction logic, MERGEs into `silver.filings_index`, `silver.financial_facts`, `silver.corporate_actions`.

Key parsing (PySpark):
```python
# Parse parallel arrays from filings.recent JSON
from_json_schema = "struct<accessionNumber:array<string>, filingDate:array<string>, ...>"

filings_df = submissions_raw \
    .withColumn("filings", F.from_json("filings_recent_json", from_json_schema)) \
    .select(
        "cik",
        F.posexplode("filings.accessionNumber").alias("pos", "accession_number"),
        # ... join other arrays by pos
    )
```

XBRL facts parsing: explode `facts_json` by taxonomy → concept → unit → data points, then filter by period duration.

### 11.6 `scripts/silver_to_gold/01_build_gold.py`

PySpark. Truncates and rebuilds all Gold tables from Silver. Run after Silver is complete.

```python
# Rebuild gold.financial_statements_annual
spark.sql("""
  CREATE OR REPLACE TABLE sec_edgar_platform.gold.financial_statements_annual
  USING DELTA
  CLUSTER BY (ticker, fiscal_year)
  AS
  SELECT
    f.security_id, f.cik, s.ticker, s.company_name, s.exchange,
    f.fiscal_year, f.period_end, f.filed_date, f.taxonomy,
    f.revenues, f.net_income, f.operating_income,
    f.total_assets, f.total_liabilities, f.stockholders_equity,
    f.long_term_debt, f.cash_and_equivalents, f.operating_cash_flow,
    f.eps_basic, f.eps_diluted,
    ROUND(f.net_income / NULLIF(f.revenues, 0), 6)             AS net_margin,
    ROUND(f.net_income / NULLIF(f.stockholders_equity, 0), 6)  AS return_on_equity,
    ROUND(f.long_term_debt / NULLIF(f.stockholders_equity, 0), 6) AS debt_to_equity,
    current_timestamp() AS refreshed_at
  FROM sec_edgar_platform.silver.financial_facts f
  JOIN sec_edgar_platform.silver.dim_security s ON s.security_id = f.security_id
  WHERE f.form_type IN ('10-K', '10-K/A')
    AND f.fiscal_period = 'FY'
    AND s.active_flag = TRUE
""")
```

---

## 12. Databricks Workflow YAML

Save as `workflows/sec_edgar_daily.yml`. Deploy with `databricks bundle deploy`.

```yaml
# workflows/sec_edgar_daily.yml
bundle:
  name: sec_edgar_platform

resources:
  jobs:
    sec_edgar_daily:
      name: "SEC EDGAR Daily Pipeline"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"    # 06:00 UTC daily
        timezone_id: "UTC"
      job_clusters:
        - job_cluster_key: silver_gold_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"            # change for your cloud
            num_workers: 2
            spark_conf:
              spark.databricks.delta.optimizeWrite.enabled: "true"

      tasks:
        - task_key: ingest_tickers
          description: "Download company_tickers_exchange.json → Parquet → Bronze"
          spark_python_task:
            python_file: "scripts/ingest/01_ingest_tickers_exchange.py"
          libraries:
            - pypi: { package: "requests" }
            - pypi: { package: "pyarrow" }
            - pypi: { package: "pandas" }

        - task_key: ingest_submissions
          description: "Fetch submissions JSON per CIK → Parquet → Bronze"
          depends_on: [{ task_key: ingest_tickers }]
          spark_python_task:
            python_file: "scripts/ingest/02_ingest_submissions.py"
          libraries:
            - pypi: { package: "requests" }
            - pypi: { package: "pyarrow" }

        - task_key: ingest_companyfacts
          description: "Fetch companyfacts JSON per CIK → Parquet → Bronze"
          depends_on: [{ task_key: ingest_tickers }]
          spark_python_task:
            python_file: "scripts/ingest/03_ingest_companyfacts.py"
          libraries:
            - pypi: { package: "requests" }
            - pypi: { package: "pyarrow" }

        - task_key: silver_security_master
          description: "Bronze → Silver: dim_security + security_xref"
          depends_on: [{ task_key: ingest_tickers }]
          job_cluster_key: silver_gold_cluster
          notebook_task:
            notebook_path: "scripts/bronze_to_silver/01_silver_dim_security"

        - task_key: silver_filings_facts
          description: "Bronze → Silver: filings_index + financial_facts + corporate_actions"
          depends_on:
            - { task_key: ingest_submissions }
            - { task_key: ingest_companyfacts }
            - { task_key: silver_security_master }
          job_cluster_key: silver_gold_cluster
          notebook_task:
            notebook_path: "scripts/bronze_to_silver/02_silver_filings_and_facts"

        - task_key: build_gold
          description: "Silver → Gold: all analytics tables"
          depends_on: [{ task_key: silver_filings_facts }]
          job_cluster_key: silver_gold_cluster
          notebook_task:
            notebook_path: "scripts/silver_to_gold/01_build_gold"
```

---

## 13. Project File Layout

```
sec_edgar_platform/
├── config/
│   └── settings.py                         ← EDIT THIS: STORAGE_ROOT, USER_AGENT
├── scripts/
│   ├── setup/
│   │   ├── 00_unity_catalog_setup.sql      ← run once as admin
│   │   ├── 01_create_bronze_tables.sql
│   │   ├── 02_create_silver_tables.sql
│   │   └── 03_create_gold_tables.sql
│   ├── ingest/
│   │   ├── 01_ingest_tickers_exchange.py   ← single-node Python
│   │   ├── 02_ingest_submissions.py
│   │   └── 03_ingest_companyfacts.py
│   ├── bronze_to_silver/
│   │   ├── 01_silver_dim_security.py       ← Databricks notebook/PySpark
│   │   └── 02_silver_filings_and_facts.py
│   └── silver_to_gold/
│       └── 01_build_gold.py
├── workflows/
│   └── sec_edgar_daily.yml                 ← Databricks Asset Bundle workflow
├── tests/
│   └── test_spot_check.py                  ← verification queries
└── README.md
```

---

## 14. Verification Steps

### Step 1 — Unity Catalog setup
```sql
-- Run in Databricks SQL editor
SHOW SCHEMAS IN sec_edgar_platform;
-- Expected: bronze, silver, gold
```

### Step 2 — DDL tables created
```sql
SHOW TABLES IN sec_edgar_platform.bronze;
SHOW TABLES IN sec_edgar_platform.silver;
SHOW TABLES IN sec_edgar_platform.gold;
```

### Step 3 — Smoke test ingestion (5 CIKs)
```bash
# Run locally or on a Databricks single-node cluster
python scripts/ingest/01_ingest_tickers_exchange.py
# Expected: Parquet written to {STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date=<today>/

# Check row count
python -c "import pyarrow.parquet as pq; t = pq.read_table('{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date=<today>/data.parquet'); print(len(t))"
# Expected: ~10,000–12,000 rows
```

### Step 4 — Bronze Delta tables loaded
```sql
SELECT ingestion_date, COUNT(*) AS rows
FROM sec_edgar_platform.bronze.company_tickers_exchange_raw
GROUP BY ingestion_date ORDER BY ingestion_date DESC LIMIT 5;
```

### Step 5 — Silver security master populated
```sql
SELECT COUNT(*) AS total_securities,
       SUM(CASE WHEN active_flag THEN 1 ELSE 0 END) AS active
FROM sec_edgar_platform.silver.dim_security;
-- Expected: ~6,000–8,000 active (NYSE + Nasdaq)
```

### Step 6 — Spot check known tickers
```sql
-- AAPL, MSFT, TSLA, JPM should all exist with recent financial facts
SELECT s.ticker, s.company_name, f.period_end, f.revenues, f.net_income
FROM sec_edgar_platform.silver.dim_security s
JOIN sec_edgar_platform.silver.financial_facts f ON s.security_id = f.security_id
WHERE s.ticker IN ('AAPL', 'MSFT', 'TSLA', 'JPM')
  AND f.form_type = '10-K'
ORDER BY s.ticker, f.period_end DESC;
```

### Step 7 — Gold tables ready
```sql
SELECT ticker, fiscal_year, revenues, net_margin, return_on_equity
FROM sec_edgar_platform.gold.financial_statements_annual
WHERE ticker IN ('AAPL', 'MSFT')
ORDER BY ticker, fiscal_year DESC;
```

### Step 8 — Full pipeline run
```bash
databricks bundle deploy --target prod
databricks bundle run sec_edgar_daily
# Monitor in Databricks Jobs UI
# Expected runtime: ~60–90 min for full initial load of ~8,000 companies
```

### Step 9 — Idempotency check
Re-run the pipeline for the same date. Row counts in Silver/Gold should be identical (MERGE is idempotent). Bronze will skip (parquet files already exist).

---

## 15. Edge Cases and Error Handling

| Scenario | Handling |
|---|---|
| Company uses IFRS (not us-gaap) | Try us-gaap facts first, then ifrs-full; set `taxonomy = 'ifrs-full'` |
| `companyfacts` returns 404 | Log CIK to checkpoint; write NULL financial facts; do not retry on same day |
| Multi-class shares (BRK.A/BRK.B) | Both share same CIK; `ticker_class` differentiates; both get separate `security_id` |
| HTTP 429 rate limit | Sleep 60s, retry up to 3× with exponential backoff |
| `submissions.filings.files` pagination | Fetch continuation pages, cap at 5 pages per CIK |
| Delta schema mismatch | Use `mergeSchema = false` on Bronze append; fail loudly if schema changed upstream |
| SEC site maintenance (weekends) | Pipeline is idempotent — next run will backfill missing date |
| Large companyfacts payload (>10 MB) | Store as-is in Parquet STRING column; parse in Silver job where Spark has memory |

---

## 16. How to Start Coding (Post-Review Checklist)

Once you have reviewed this spec and are ready to implement, follow these steps in order. Each step is independent and testable before moving to the next.

### Phase 0 — Environment Setup (Do Once)

- [ ] **Create a Databricks workspace** (any cloud). Enable Unity Catalog.
- [ ] **Provision object storage** (S3 bucket / ADLS container / GCS bucket). Note the root path.
- [ ] **Edit `config/settings.py`** — set `STORAGE_ROOT`, `USER_AGENT`, `CATALOG_NAME`.
- [ ] **Create Python virtual environment** for the ingestion driver:
  ```bash
  python -m venv .venv && source .venv/bin/activate
  pip install requests pyarrow pandas
  ```
- [ ] **Configure cloud credentials** in the environment (AWS profile / ADLS SAS / GCS service account) so `pyarrow` can write to `STORAGE_ROOT`.
- [ ] **Install Databricks CLI** and authenticate: `databricks auth login`.

### Phase 1 — Create the Project Scaffold

Create the directory structure from Section 13:
```bash
mkdir -p config scripts/{setup,ingest,bronze_to_silver,silver_to_gold} workflows tests
touch config/settings.py
```

Copy the settings template from **Section 4** into `config/settings.py` and fill in your values.

**First file to write:** `config/settings.py`

### Phase 2 — Run the DDL (Unity Catalog + Tables)

1. Open a Databricks SQL editor or notebook.
2. Run `scripts/setup/00_unity_catalog_setup.sql` — creates catalog + schemas + storage location. Content: **Section 5**.
3. Run `scripts/setup/01_create_bronze_tables.sql` — all Bronze DDL. Content: **Section 7**.
4. Run `scripts/setup/02_create_silver_tables.sql` — all Silver DDL. Content: **Section 8**.
5. Run `scripts/setup/03_create_gold_tables.sql` — all Gold DDL. Content: **Section 9**.

**Verify:** `SHOW TABLES IN sec_edgar_platform.bronze;` returns 4 tables.

### Phase 3 — Write and Test the Ingestion Scripts

Write scripts in this order (each can be tested independently):

1. **`scripts/ingest/01_ingest_tickers_exchange.py`** (Section 11.1)
   - Test: `python scripts/ingest/01_ingest_tickers_exchange.py`
   - Expected: Parquet written to `{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date=<today>/data.parquet`
   - Check row count ≈ 10,000–12,000

2. **`scripts/ingest/02_ingest_submissions.py`** (Section 11.2)
   - Test with 5 CIKs first: add a `--limit 5` flag during development
   - Expected: 5 Parquet files under `{STORAGE_ROOT}/bronze/submissions/ingestion_date=<today>/`

3. **`scripts/ingest/03_ingest_companyfacts.py`** (Section 11.3)
   - Same pattern as submissions
   - Test with AAPL CIK (320193) first — it's a large payload, good stress test

### Phase 4 — Load Bronze Delta Tables

After each ingestion script, load the Parquet files into the Bronze Delta tables. Write a Databricks notebook for each (or add a `load_bronze()` function to each ingest script):

```python
# Example: load tickers into Bronze
spark.read.parquet(f"{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={today}/") \
    .write.format("delta").mode("append") \
    .saveAsTable("sec_edgar_platform.bronze.company_tickers_exchange_raw")
```

**Verify:** `SELECT COUNT(*) FROM sec_edgar_platform.bronze.company_tickers_exchange_raw;`

### Phase 5 — Write Silver Transforms (Databricks Notebooks)

Write in order:

1. **`scripts/bronze_to_silver/01_silver_dim_security.py`** (Section 11.4)
   - The `security_id` generation logic is the most critical piece — test it with known CIKs
   - Verify: AAPL, MSFT, TSLA all have rows in `silver.dim_security`

2. **`scripts/bronze_to_silver/02_silver_filings_and_facts.py`** (Section 11.5)
   - Start with just `filings_index` (simpler parsing), then add `financial_facts`
   - Key complexity: parsing parallel arrays from `filings.recent` JSON and XBRL concept extraction
   - Verify: `SELECT * FROM silver.financial_facts WHERE cik = 320193 AND form_type = '10-K' ORDER BY period_end DESC LIMIT 5`

### Phase 6 — Write Gold Transforms

**`scripts/silver_to_gold/01_build_gold.py`** (Section 11.6)

The Gold layer is entirely SQL `CREATE OR REPLACE TABLE ... AS SELECT`. Write one statement per Gold table. Start with `financial_statements_annual` — it's the most valuable for analytics.

**Verify:** `SELECT ticker, fiscal_year, revenues, net_margin FROM gold.financial_statements_annual WHERE ticker = 'AAPL' ORDER BY fiscal_year DESC LIMIT 5`

### Phase 7 — Wire Up the Databricks Workflow

1. Create `workflows/sec_edgar_daily.yml` from **Section 12**.
2. Update `node_type_id` to match your cloud's available instance types.
3. Deploy: `databricks bundle deploy --target dev`
4. Run manually: `databricks bundle run sec_edgar_daily`
5. Monitor in the Databricks Jobs UI.

### Phase 8 — Full Verification

Run all verification queries from **Section 14** in order. The final check is the idempotency test (Step 9).

---

### Implementation Priority (if you want to deliver value incrementally)

| Priority | Deliverable | Sections |
|---|---|---|
| 1 (highest) | Bronze tickers + `silver.dim_security` | 7.1, 8.1, 8.2, 11.1, 11.4 |
| 2 | Bronze submissions + `silver.filings_index` | 7.2, 8.4, 11.2 |
| 3 | Bronze companyfacts + `silver.financial_facts` | 7.3, 8.5, 11.3, 11.5 |
| 4 | Gold annual financials | 9.1, 11.6 |
| 5 | Gold quarterly + company profile + corporate events | 9.2–9.5 |
| 6 | Databricks Workflow automation | 12 |

**Start with Priority 1.** You'll have a queryable security master with `security_id` before writing a single financial fact row.
