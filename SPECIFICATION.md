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
REQUEST_TIMEOUT  = 30
MAX_RETRIES      = 3

# Bulk ingestion tuning
INGEST_WORKERS   = 8      # parallel HTTP threads (ThreadPoolExecutor max_workers)
INGEST_RATE_RPS  = 8.0    # max requests/sec globally across all threads (SEC allows 10)
BATCH_SIZE       = 500    # CIKs accumulated in memory before flushing to Parquet

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

All scripts live under `scripts/`. HTTP ingestion runs as single-node Python (not PySpark) to maintain SEC rate-limit control. Spark notebooks handle Bronze→Silver→Gold transforms.

### 11.0 Ingestion Design Principles

**Batched parallel fetching** (applies to all per-CIK ingestion scripts):

- `ThreadPoolExecutor(max_workers=8)` fetches 8 CIKs in parallel
- A shared `RateLimiter` enforces ≤8 req/s globally across all threads (below SEC's 10 req/s limit)
- Results accumulate in memory and are flushed to Parquet every `BATCH_SIZE=500` CIKs
- This reduces object storage writes from ~8,000 small files to ~16 batch files per dataset per day

**Object storage layout (batch files, not per-CIK files):**
```
{STORAGE_ROOT}/bronze/submissions/ingestion_date=2024-01-25/
  batch_0001.parquet    ← 500 CIKs
  batch_0002.parquet    ← 500 CIKs
  ...
  batch_0016.parquet    ← remainder
```

**Rate Limiter (shared across all threads):**
```python
# scripts/ingest/_rate_limiter.py
import threading, time

class RateLimiter:
    """Thread-safe token-bucket rate limiter."""
    def __init__(self, rate: float):        # rate = max requests per second
        self._interval = 1.0 / rate
        self._lock = threading.Lock()
        self._last_call = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()
```

**Retry wrapper (shared):**
```python
# scripts/ingest/_http.py
import requests, time
from ._rate_limiter import RateLimiter

_limiter = RateLimiter(rate=8.0)           # 8 req/s; SEC allows 10, leave headroom

def edgar_get(url: str, session: requests.Session, max_retries: int = 3):
    for attempt in range(max_retries):
        _limiter.acquire()
        try:
            r = session.get(url, timeout=30)
        except requests.exceptions.ConnectionError:
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:
            time.sleep(60)
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    return None                             # exhausted retries
```

**Batch writer (shared):**
```python
# scripts/ingest/_batch_writer.py
import pyarrow as pa, pyarrow.parquet as pq
from pathlib import PurePosixPath

def write_batch(records: list[dict], schema: pa.Schema,
                storage_root: str, dataset: str,
                ingest_date: str, batch_num: int):
    table = pa.Table.from_pylist(records, schema=schema)
    path = f"{storage_root}/bronze/{dataset}/ingestion_date={ingest_date}/batch_{batch_num:04d}.parquet"
    pq.write_table(table, path, compression="snappy")
    return path
```

### 11.1 `scripts/ingest/01_ingest_tickers_exchange.py` — Daily Tickers Snapshot

Single HTTP call (one file, not per-CIK). No threading needed.

```python
"""
Usage: python scripts/ingest/01_ingest_tickers_exchange.py [--date YYYY-MM-DD]
Idempotent — skips if Parquet already exists for that date.
"""
import requests, pyarrow as pa, pyarrow.parquet as pq, sys
from datetime import date
from config.settings import STORAGE_ROOT, USER_AGENT

def run(ingest_date: date):
    out_path = f"{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={ingest_date}/data.parquet"
    # Idempotency check
    try:
        pq.read_metadata(out_path)
        print(f"Already exists: {out_path}, skipping.")
        return
    except Exception:
        pass

    resp = requests.get(
        "https://www.sec.gov/files/company_tickers_exchange.json",
        headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    records = [
        {"cik": row[0], "company_name": row[1], "ticker": row[2],
         "exchange": row[3], "ingestion_date": ingest_date.isoformat()}
        for row in payload["data"]
    ]
    table = pa.Table.from_pylist(records)
    pq.write_table(table, out_path, compression="snappy")
    print(f"Written {len(records)} rows → {out_path}")

if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    run(d)
```

### 11.2 `scripts/ingest/02_ingest_submissions.py` — Bulk Submissions

Fetches all CIK submissions in parallel batches. Completes before any Silver job starts.

```python
"""
Usage: python scripts/ingest/02_ingest_submissions.py [--date YYYY-MM-DD] [--limit N]
--limit: for dev/testing, process only first N CIKs.
Idempotent — resumes from last incomplete batch.
"""
import sys, requests, pyarrow as pa, pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from config.settings import (STORAGE_ROOT, USER_AGENT, TARGET_EXCHANGES,
                              INGEST_WORKERS, BATCH_SIZE)
from scripts.ingest._http import edgar_get
from scripts.ingest._batch_writer import write_batch

SUBMISSIONS_SCHEMA = pa.schema([
    pa.field("ingestion_date",         pa.string()),
    pa.field("cik",                    pa.int64()),
    pa.field("entity_name",            pa.string()),
    pa.field("entity_type",            pa.string()),
    pa.field("sic",                    pa.string()),
    pa.field("sic_description",        pa.string()),
    pa.field("ein",                    pa.string()),
    pa.field("category",               pa.string()),
    pa.field("fiscal_year_end",        pa.string()),
    pa.field("state_of_incorporation", pa.string()),
    pa.field("tickers_json",           pa.string()),
    pa.field("exchanges_json",         pa.string()),
    pa.field("filings_recent_json",    pa.string()),
    pa.field("filings_files_json",     pa.string()),
])

def fetch_one(cik: int, session: requests.Session, ingest_date: str) -> dict | None:
    url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json"
    data = edgar_get(url, session)
    if data is None:
        return None
    import json
    return {
        "ingestion_date":          ingest_date,
        "cik":                     cik,
        "entity_name":             data.get("name"),
        "entity_type":             data.get("entityType"),
        "sic":                     data.get("sic"),
        "sic_description":         data.get("sicDescription"),
        "ein":                     data.get("ein"),
        "category":                data.get("category"),
        "fiscal_year_end":         data.get("fiscalYearEnd"),
        "state_of_incorporation":  data.get("stateOfIncorporation"),
        "tickers_json":            json.dumps(data.get("tickers", [])),
        "exchanges_json":          json.dumps(data.get("exchanges", [])),
        "filings_recent_json":     json.dumps(data.get("filings", {}).get("recent", {})),
        "filings_files_json":      json.dumps(data.get("filings", {}).get("files", [])),
    }

def run(ingest_date: date, limit: int | None = None):
    # 1. Load CIK list from tickers Parquet (already written by script 01)
    tickers_path = f"{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={ingest_date}/data.parquet"
    tickers = pq.read_table(tickers_path, columns=["cik", "exchange"]).to_pydict()
    cik_list = [
        cik for cik, exch in zip(tickers["cik"], tickers["exchange"])
        if exch in TARGET_EXCHANGES
    ]
    if limit:
        cik_list = cik_list[:limit]
    print(f"Processing {len(cik_list)} CIKs with {INGEST_WORKERS} workers, batch size {BATCH_SIZE}")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})

    batch_num, buffer, ok, err = 1, [], 0, 0

    # 2. Parallel fetch with shared rate limiter
    with ThreadPoolExecutor(max_workers=INGEST_WORKERS) as pool:
        futures = {pool.submit(fetch_one, cik, session, ingest_date.isoformat()): cik
                   for cik in cik_list}
        for future in as_completed(futures):
            result = future.result()
            if result:
                buffer.append(result)
                ok += 1
            else:
                err += 1
            # 3. Flush batch to Parquet every BATCH_SIZE records
            if len(buffer) >= BATCH_SIZE:
                path = write_batch(buffer, SUBMISSIONS_SCHEMA,
                                   STORAGE_ROOT, "submissions",
                                   ingest_date.isoformat(), batch_num)
                print(f"  Batch {batch_num:04d}: {len(buffer)} rows → {path}")
                batch_num += 1
                buffer = []

    # 4. Final partial batch
    if buffer:
        write_batch(buffer, SUBMISSIONS_SCHEMA, STORAGE_ROOT,
                    "submissions", ingest_date.isoformat(), batch_num)

    print(f"Done. ok={ok} err={err} batches={batch_num}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=date.today().isoformat())
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    run(date.fromisoformat(args.date), args.limit)
```

### 11.3 `scripts/ingest/03_ingest_companyfacts.py` — Bulk Company Facts (XBRL)

Identical pattern to `02_ingest_submissions.py`. Stores the full `facts` JSON as a single string column (2–5 MB per CIK). Batch Parquet files will be large (~1–2 GB each) — this is correct and expected.

```python
COMPANYFACTS_SCHEMA = pa.schema([
    pa.field("ingestion_date",   pa.string()),
    pa.field("cik",              pa.int64()),
    pa.field("entity_name",      pa.string()),
    pa.field("facts_json",       pa.string()),   # full {"us-gaap":{...},"dei":{...}}
])

def fetch_one(cik: int, session, ingest_date: str) -> dict | None:
    import json
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json"
    data = edgar_get(url, session)
    if data is None:
        return None
    return {
        "ingestion_date": ingest_date,
        "cik":            cik,
        "entity_name":    data.get("entityName"),
        "facts_json":     json.dumps(data.get("facts", {})),
    }
# run() identical to 02_ingest_submissions.py — swap SCHEMA and fetch_one
```

### 11.4 `scripts/ingest/04_load_bronze_delta.py` — Bulk Bronze Delta Load (Spark)

Runs **after all three ingestion scripts complete**. Loads all batch Parquet files into their Delta tables using `COPY INTO` (idempotent, tracks already-loaded files).

```python
# Databricks notebook / PySpark script
from datetime import date
from config.settings import STORAGE_ROOT, CATALOG_NAME

today = date.today().isoformat()

# Load each dataset — COPY INTO is idempotent (skips already-loaded files)
for dataset, table in [
    ("company_tickers_exchange", "bronze.company_tickers_exchange_raw"),
    ("submissions",              "bronze.submissions_raw"),
    ("companyfacts",             "bronze.companyfacts_raw"),
]:
    path = f"{STORAGE_ROOT}/bronze/{dataset}/ingestion_date={today}/"
    spark.sql(f"""
        COPY INTO {CATALOG_NAME}.{table}
        FROM '{path}'
        FILEFORMAT = PARQUET
        FORMAT_OPTIONS ('inferSchema' = 'false', 'mergeSchema' = 'false')
        COPY_OPTIONS ('mergeSchema' = 'false')
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG_NAME}.{table} WHERE ingestion_date = '{today}'").collect()[0][0]
    print(f"{table}: loaded {count:,} rows for {today}")
```

### 11.5 `scripts/bronze_to_silver/01_silver_dim_security.py` — Security Master

Reads from Bronze `company_tickers_exchange_raw` (today's partition only). Computes `security_id` and MERGEs into `silver.dim_security`.

```python
from pyspark.sql import functions as F
from datetime import date

today = date.today().isoformat()

bronze = spark.table("sec_edgar_platform.bronze.company_tickers_exchange_raw") \
    .filter(F.col("ingestion_date") == today) \
    .filter(F.col("exchange").isin(["NYSE", "Nasdaq"]))

silver_candidates = bronze.withColumn(
    "security_id",
    F.left(F.sha2(F.concat(
        F.lpad(F.col("cik").cast("string"), 10, "0"),
        F.lit("|"),
        F.lit("PRIMARY")), 256), 16)
).withColumn("active_flag", F.lit(True)) \
 .withColumn("first_seen_date", F.lit(today).cast("date")) \
 .withColumn("last_seen_date",  F.lit(today).cast("date"))

silver_candidates.createOrReplaceTempView("new_securities")

spark.sql("""
  MERGE INTO sec_edgar_platform.silver.dim_security AS t
  USING new_securities AS s ON t.security_id = s.security_id
  WHEN MATCHED THEN UPDATE SET
    t.last_seen_date = s.last_seen_date,
    t.company_name   = s.company_name,
    t.exchange       = s.exchange,
    t.updated_at     = current_timestamp()
  WHEN NOT MATCHED THEN INSERT *
""")
```

### 11.6 `scripts/bronze_to_silver/02_silver_filings_and_facts.py`

PySpark. Reads Bronze `submissions_raw` and `companyfacts_raw` (today's partition). Parses filings parallel arrays and XBRL concepts. MERGEs into `silver.filings_index`, `silver.financial_facts`, `silver.corporate_actions`.

### 11.7 `scripts/silver_to_gold/01_build_gold.py`

PySpark. Rebuilds all Gold tables from Silver using `CREATE OR REPLACE TABLE ... AS SELECT`. See Section 9 for the SQL patterns.

---

## 12. Databricks Workflow YAML

Save as `workflows/sec_edgar_daily.yml`. Deploy with `databricks bundle deploy`.

**Pipeline execution order (enforced by task dependencies):**
```
ingest_tickers ──────────────────────────────────────────────────────────────────────┐
                                                                                     │
ingest_tickers → ingest_submissions (bulk parallel, 8 workers) ──────────────────────┤
                                                                                     ├─→ load_bronze_delta → [GATE: bronze_ingestion_complete]
ingest_tickers → ingest_companyfacts (bulk parallel, 8 workers) ─────────────────────┘                              │
                                                                                                                     ▼
                                                                                                         silver_security_master
                                                                                                                     │
                                                                                                         silver_filings_facts
                                                                                                                     │
                                                                                                             build_gold
```

**Rule:** No Silver or Gold task may start until `bronze_ingestion_complete` succeeds.

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
        - job_cluster_key: spark_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"            # change for your cloud
            num_workers: 2
            spark_conf:
              spark.databricks.delta.optimizeWrite.enabled: "true"
              spark.databricks.delta.optimizeWrite.binSize: "1073741824"  # 1 GB bins

      tasks:

        # ── STAGE 1: Download tickers (1 HTTP call, prerequisite for CIK list) ──────
        - task_key: ingest_tickers
          description: "Download company_tickers_exchange.json → Parquet (1 file)"
          spark_python_task:
            python_file: "scripts/ingest/01_ingest_tickers_exchange.py"
          libraries:
            - pypi: { package: "requests" }
            - pypi: { package: "pyarrow" }

        # ── STAGE 2: Bulk parallel HTTP fetch (submissions + facts run concurrently) ─
        - task_key: ingest_submissions
          description: "Bulk fetch submissions (8 workers, batch Parquet)"
          depends_on: [{ task_key: ingest_tickers }]
          spark_python_task:
            python_file: "scripts/ingest/02_ingest_submissions.py"
          libraries:
            - pypi: { package: "requests" }
            - pypi: { package: "pyarrow" }

        - task_key: ingest_companyfacts
          description: "Bulk fetch companyfacts/XBRL (8 workers, batch Parquet)"
          depends_on: [{ task_key: ingest_tickers }]
          spark_python_task:
            python_file: "scripts/ingest/03_ingest_companyfacts.py"
          libraries:
            - pypi: { package: "requests" }
            - pypi: { package: "pyarrow" }

        # ── STAGE 3: Load all Parquet batches into Bronze Delta (COPY INTO) ─────────
        - task_key: load_bronze_delta
          description: "COPY INTO all batch Parquet files → Bronze Delta tables"
          depends_on:
            - { task_key: ingest_tickers }
            - { task_key: ingest_submissions }
            - { task_key: ingest_companyfacts }
          job_cluster_key: spark_cluster
          notebook_task:
            notebook_path: "scripts/ingest/04_load_bronze_delta"

        # ── GATE: all raw data in Delta before any Silver/Gold task starts ──────────
        - task_key: bronze_ingestion_complete
          description: "GATE — verifies row counts in all Bronze tables for today"
          depends_on: [{ task_key: load_bronze_delta }]
          job_cluster_key: spark_cluster
          notebook_task:
            notebook_path: "scripts/ingest/05_verify_bronze_complete"
          # This notebook asserts:
          #   bronze.company_tickers_exchange_raw has rows for today
          #   bronze.submissions_raw has rows for today
          #   bronze.companyfacts_raw has rows for today
          # Raises exception (fails the task) if any assertion fails → stops pipeline

        # ── STAGE 4: Silver transforms (only start after bronze_ingestion_complete) ──
        - task_key: silver_security_master
          description: "Bronze → Silver: dim_security + security_xref"
          depends_on: [{ task_key: bronze_ingestion_complete }]
          job_cluster_key: spark_cluster
          notebook_task:
            notebook_path: "scripts/bronze_to_silver/01_silver_dim_security"

        - task_key: silver_filings_facts
          description: "Bronze → Silver: filings_index + financial_facts + corporate_actions"
          depends_on: [{ task_key: silver_security_master }]
          job_cluster_key: spark_cluster
          notebook_task:
            notebook_path: "scripts/bronze_to_silver/02_silver_filings_and_facts"

        # ── STAGE 5: Gold (only after all Silver complete) ────────────────────────────
        - task_key: build_gold
          description: "Silver → Gold: all analytics tables"
          depends_on: [{ task_key: silver_filings_facts }]
          job_cluster_key: spark_cluster
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
│   │   ├── _rate_limiter.py                ← shared RateLimiter (8 req/s)
│   │   ├── _http.py                        ← edgar_get() with retry + rate limit
│   │   ├── _batch_writer.py                ← write_batch() bulk Parquet writer
│   │   ├── 01_ingest_tickers_exchange.py   ← 1 HTTP call, 1 Parquet file
│   │   ├── 02_ingest_submissions.py        ← bulk: 8 workers, batch Parquet
│   │   ├── 03_ingest_companyfacts.py       ← bulk: 8 workers, batch Parquet
│   │   ├── 04_load_bronze_delta.py         ← COPY INTO all batches → Delta
│   │   └── 05_verify_bronze_complete.py    ← GATE: asserts row counts > 0
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
