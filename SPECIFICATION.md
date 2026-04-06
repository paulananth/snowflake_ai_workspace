# SEC EDGAR Security & Financial Data Platform — SPECIFICATION.md

**Platform:** Cloud-agnostic (AWS or Azure)  **Architecture:** Medallion (Bronze / Silver / Gold)  **Source:** SEC EDGAR only  **Storage:** Parquet files in S3 or Azure ADLS Gen2

## 1. System Overview

This is a **greenfield** cloud-agnostic platform for ingesting, storing, and serving SEC EDGAR financial data. It runs on **AWS** (S3 + ECS Fargate + Step Functions) or **Azure** (ADLS Gen2 + Azure Batch + ADF) — configured by a single `CLOUD_PROVIDER` variable. No Databricks, no Spark cluster, no managed database required.

**Goals:**
- Store all raw SEC EDGAR API responses as Parquet files in object storage (auditable, reproducible)
- Build a curated security master with a stable surrogate key (`security_id`)
- Parse XBRL financial facts into analytics-ready Parquet using DuckDB as a stateless SQL engine
- Support AWS S3 or Azure ADLS Gen2 via a single `STORAGE_ROOT` config variable
- Serve as the foundation for a security and financial analysis system

---

## 2. Architecture Overview

```
SEC EDGAR APIs
     │
     ▼
[Python Ingestion Driver]  ← single-node per task, rate-limited (≤8 req/s per task, sequential tasks)
     │  HTTP → Parquet (pyarrow + s3fs / adlfs)
     ▼
Object Storage  ─────────────── single source of truth (no separate database)
  AWS:   s3://{BUCKET}/{PREFIX}/
  Azure: abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/{PREFIX}/
    bronze/   ← append-only Parquet (raw API responses, never modified)
    silver/   ← Parquet written by DuckDB transform tasks
    gold/     ← Parquet written by DuckDB transform tasks
     │
     ▼ (DuckDB as stateless per-task in-memory query engine)
Each container task:
  reads input Parquet from S3/Azure → runs SQL transform in RAM → writes output Parquet to S3/Azure
```

**Key design principles:**
- No cluster, no managed database — DuckDB runs in a single Docker container
- All state lives in object storage as Parquet — containers are fully ephemeral
- Bronze is append-only (never updated, full audit trail)
- Silver uses per-date snapshot Parquet (idempotent: overwrite today's partition on re-run)
- Gold is rebuilt from Silver on each run (no incremental complexity)
- `security_id` is a deterministic 16-char hex hash — stable across re-runs, no sequences needed
- Ingest tasks run **sequentially** to stay under SEC's 10 req/s total rate limit

---

## 3. Prerequisites

**AWS:**
1. **S3 bucket** in your target region (e.g. `my-sec-edgar-bucket`)
2. **IAM role** with `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `arn:aws:s3:::my-sec-edgar-bucket/sec-edgar/*`
3. **ECR repository** to store the Docker image
4. **ECS cluster** (Fargate launch type) + VPC with private subnets and a NAT Gateway (for SEC API access)
5. **AWS Step Functions** state machine (see Section 12)

**Azure:**
1. **Storage account** with **Hierarchical Namespace enabled** (ADLS Gen2) — required for `abfss://` scheme
2. **User-Assigned Managed Identity** granted `Storage Blob Data Contributor` on the storage account
3. **Azure Container Registry (ACR)** to store the Docker image
4. **Azure Batch account** with a pool configured for Docker container execution; pool identity = Managed Identity above
5. **Azure Data Factory** pipeline (see Section 12)

**Both clouds (local dev):**
- **Python 3.11+** and **Docker** installed locally
- **Cloud CLI** (`aws` or `az`) configured with credentials that have write access to the storage bucket/container

---

## 4. Configuration

All settings are in `config/settings.py`. Every value can be overridden by an environment variable — making the same image work locally and in cloud containers without code changes.

```python
# config/settings.py
import os
from datetime import date

# ─── Cloud provider ────────────────────────────────────────────────────────────
CLOUD_PROVIDER = os.environ.get("CLOUD_PROVIDER", "aws")   # "aws" | "azure"

# ─── AWS ───────────────────────────────────────────────────────────────────────
AWS_BUCKET      = os.environ.get("AWS_BUCKET",          "my-bucket")     # CHANGE THIS
STORAGE_PREFIX  = os.environ.get("STORAGE_PREFIX",      "sec-edgar")
AWS_REGION      = os.environ.get("AWS_DEFAULT_REGION",  "us-east-1")     # CHANGE THIS
# Auth: ECS task IAM role (production) or ~/.aws credentials (local dev) — nothing to set here

# ─── Azure ─────────────────────────────────────────────────────────────────────
AZURE_ACCOUNT   = os.environ.get("AZURE_STORAGE_ACCOUNT", "myaccount")   # CHANGE THIS
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER",       "sec-edgar")   # CHANGE THIS
# Requires ADLS Gen2 (Hierarchical Namespace enabled on the storage account)
# Auth: User-Assigned Managed Identity — set AZURE_CLIENT_ID env var on Batch pool
# Local dev: `az login` (DefaultAzureCredential picks it up automatically)

# ─── Derived storage root ──────────────────────────────────────────────────────
def _storage_root() -> str:
    if CLOUD_PROVIDER == "aws":
        return f"s3://{AWS_BUCKET}/{STORAGE_PREFIX}"
    if CLOUD_PROVIDER == "azure":
        return f"abfss://{AZURE_CONTAINER}@{AZURE_ACCOUNT}.dfs.core.windows.net/{STORAGE_PREFIX}"
    raise ValueError(f"Unknown CLOUD_PROVIDER: {CLOUD_PROVIDER!r}")

STORAGE_ROOT = _storage_root()

# ─── SEC EDGAR API ─────────────────────────────────────────────────────────────
USER_AGENT      = os.environ.get("SEC_USER_AGENT", "MyOrg DataPipeline contact@myorg.com")
REQUEST_TIMEOUT = 30
MAX_RETRIES     = 3

# ─── Ingestion tuning ──────────────────────────────────────────────────────────
INGEST_WORKERS   = 8      # parallel HTTP threads within a single task
# Ingest tasks run SEQUENTIALLY in the pipeline (not concurrently).
# Each task uses ≤8 req/s; sequential execution keeps combined rate under SEC's 10 req/s limit.
INGEST_RATE_RPS  = 8.0    # max req/s per task
BATCH_SIZE       = 500    # CIKs per Parquet batch file
TARGET_EXCHANGES = ["NYSE", "Nasdaq"]

# ─── Ingest date (injectable for backfill) ─────────────────────────────────────
INGEST_DATE = os.environ.get("INGEST_DATE", date.today().isoformat())
```

---

## 5. Storage Setup (One-Time)

No catalog or database to create. The folder hierarchy is created automatically by the first Parquet write. Run these once to provision the storage resources.

**AWS:**
```bash
# Create S3 bucket (versioning optional, encryption recommended)
aws s3 mb s3://my-sec-edgar-bucket --region us-east-1
aws s3api put-bucket-versioning --bucket my-sec-edgar-bucket \
    --versioning-configuration Status=Enabled

# Verify write access
aws s3 cp /dev/null s3://my-sec-edgar-bucket/sec-edgar/.keep
```

**Azure:**
```bash
# Create storage account with Hierarchical Namespace (ADLS Gen2) — REQUIRED for abfss://
az storage account create \
  --name myaccount \
  --resource-group my-rg \
  --location eastus \
  --sku Standard_LRS \
  --enable-hierarchical-namespace true   # <-- critical: enables ADLS Gen2

# Create the container
az storage container create \
  --name sec-edgar \
  --account-name myaccount

# Grant Managed Identity access
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <managed-identity-client-id> \
  --scope /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/myaccount
```

---

## 6. Object Storage Layout (All Three Layers)

All Bronze, Silver, and Gold data lives in object storage as Parquet. There is no separate database. Every task reads and writes exclusively to `{STORAGE_ROOT}`.

```
{STORAGE_ROOT}/
  bronze/                                  ← raw, append-only, never modified
    company_tickers_exchange/
      ingestion_date=2024-01-25/
        data.parquet                       ← full snapshot (~10,000–12,000 rows)
    submissions/
      ingestion_date=2024-01-25/
        batch_0001.parquet                 ← 500 CIKs per file
        batch_0002.parquet
        ...
        batch_0016.parquet
    companyfacts/
      ingestion_date=2024-01-25/
        batch_0001.parquet                 ← 500 CIKs (~1–2 GB per file — expected)
        ...
  silver/                                  ← written by DuckDB transforms; re-written on re-run
    dim_security/
      snapshot_date=2024-01-25/
        data.parquet
    filings_index/
      snapshot_date=2024-01-25/
        data.parquet
    financial_facts/
      snapshot_date=2024-01-25/
        data.parquet
    corporate_actions/
      snapshot_date=2024-01-25/
        data.parquet
  gold/                                    ← rebuilt from silver each run
    financial_statements_annual/
      refreshed_date=2024-01-25/
        data.parquet
    financial_statements_quarterly/
      refreshed_date=2024-01-25/
        data.parquet
    company_profile/
      refreshed_date=2024-01-25/
        data.parquet
    filing_catalog/
      refreshed_date=2024-01-25/
        data.parquet
    corporate_events/
      refreshed_date=2024-01-25/
        data.parquet
```

**Why Parquet (not JSON):** columnar compression (10–20× smaller), native support in DuckDB, S3 Select / Azure Query Acceleration for fast filtered reads, no database required.

**No DDL needed.** DuckDB infers schema from the Parquet files. There are no `CREATE TABLE` scripts to run.

---

## 7. Bronze Layer — Parquet Schema

No DDL to run. DuckDB reads these files directly from S3/Azure Blob using `read_parquet()`. Schemas are defined by the PyArrow schemas in the ingestion scripts.

### 7.1 `bronze/company_tickers_exchange/ingestion_date=*/data.parquet`

One row per company per daily snapshot. Append-only.

| Column | Type | Notes |
|---|---|---|
| `ingestion_date` | `string` | ISO date, e.g. `"2024-01-25"` (Hive partition key) |
| `cik` | `int64` | SEC CIK number |
| `company_name` | `string` | |
| `ticker` | `string` | |
| `exchange` | `string` | `"NYSE"` or `"Nasdaq"` |

### 7.2 `bronze/submissions/ingestion_date=*/batch_NNNN.parquet`

One row per CIK per daily ingestion. Filing arrays stored as JSON strings.

| Column | Type | Notes |
|---|---|---|
| `ingestion_date` | `string` | Hive partition key |
| `cik` | `int64` | |
| `entity_name` | `string` | |
| `entity_type` | `string` | e.g. `"operating"` |
| `sic` | `string` | |
| `sic_description` | `string` | |
| `ein` | `string` | |
| `category` | `string` | e.g. `"Large accelerated filer"` |
| `fiscal_year_end` | `string` | MMDD format, e.g. `"0928"` |
| `state_of_incorporation` | `string` | |
| `tickers_json` | `string` | JSON array: `["AAPL"]` |
| `exchanges_json` | `string` | JSON array: `["Nasdaq"]` |
| `filings_recent_json` | `string` | full `filings.recent` object as JSON |
| `filings_files_json` | `string` | pagination array as JSON |

### 7.3 `bronze/companyfacts/ingestion_date=*/batch_NNNN.parquet`

One row per CIK per daily ingestion. Full XBRL facts payload as a JSON string (2–5 MB each).

| Column | Type | Notes |
|---|---|---|
| `ingestion_date` | `string` | Hive partition key |
| `cik` | `int64` | |
| `entity_name` | `string` | |
| `facts_json` | `string` | full `{"us-gaap":{...},"dei":{...}}` JSON |

---

## 8. Silver Layer — Parquet Schema

Silver Parquet files are written by DuckDB transform scripts. Re-running a day's transform overwrites that day's `snapshot_date=` partition (idempotent).

### 8.1 `silver/dim_security/snapshot_date=*/data.parquet` — Security Master

| Column | Type | Notes |
|---|---|---|
| `security_id` | `string` | 16-char hex — see generation below |
| `cik` | `int64` | |
| `ticker` | `string` | |
| `ticker_class` | `string` | NULL = primary; `'A'`,`'B'` = multi-class shares |
| `company_name` | `string` | |
| `exchange` | `string` | |
| `sic` | `string` | |
| `entity_type` | `string` | |
| `active_flag` | `boolean` | |
| `first_seen_date` | `date` | |
| `last_seen_date` | `date` | |
| `created_at` | `timestamp` | |
| `updated_at` | `timestamp` | |

**`security_id` generation (Python):**
```python
import hashlib

def make_security_id(cik: int, ticker_class: str | None) -> str:
    tc = (ticker_class or "").upper().strip() or "PRIMARY"
    key = f"{str(cik).zfill(10)}|{tc}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]
```

**`security_id` generation (DuckDB SQL):**
```sql
left(sha256(lpad(cast(cik as varchar), 10, '0') || '|' || upper(coalesce(ticker_class, 'PRIMARY'))), 16)
```

### 8.2 `silver/filings_index/snapshot_date=*/data.parquet`

| Column | Type | Notes |
|---|---|---|
| `security_id` | `string` | |
| `cik` | `int64` | |
| `accession_number` | `string` | |
| `form_type` | `string` | `'10-K'`, `'10-Q'`, `'8-K'`, etc. |
| `filed_date` | `date` | |
| `period_of_report` | `date` | |
| `filing_url` | `string` | `https://www.sec.gov/Archives/edgar/data/{cik}/{accn}/` |
| `primary_document` | `string` | |
| `is_xbrl` | `boolean` | |
| `items` | `string` | 8-K items, e.g. `'2.01,9.01'` |

### 8.3 `silver/financial_facts/snapshot_date=*/data.parquet`

| Column | Type | Notes |
|---|---|---|
| `security_id` | `string` | |
| `cik` | `int64` | |
| `period_end` | `date` | |
| `period_start` | `date` | NULL for instant (balance sheet) concepts |
| `form_type` | `string` | `'10-K'`, `'10-Q'`, `'10-K/A'`, `'10-Q/A'` |
| `filed_date` | `date` | |
| `fiscal_year` | `int32` | |
| `fiscal_period` | `string` | `'FY'`, `'Q1'`–`'Q4'` |
| `taxonomy` | `string` | `'us-gaap'` or `'ifrs-full'` |
| `revenues` | `double` | |
| `net_income` | `double` | |
| `operating_income` | `double` | |
| `total_assets` | `double` | |
| `total_liabilities` | `double` | |
| `stockholders_equity` | `double` | |
| `long_term_debt` | `double` | |
| `cash_and_equivalents` | `double` | |
| `shares_outstanding` | `int64` | |
| `operating_cash_flow` | `double` | |
| `eps_basic` | `double` | |
| `eps_diluted` | `double` | |

### 8.4 `silver/corporate_actions/snapshot_date=*/data.parquet`

| Column | Type | `event_type` vocabulary |
|---|---|---|
| `security_id` | `string` | `'merger_acquisition'` |
| `cik` | `int64` | `'bankruptcy_filing'` |
| `event_type` | `string` | `'stock_split'` |
| `event_date` | `date` | `'reverse_split'` |
| `effective_date` | `date` | `'deregistration'` |
| `accession_number` | `string` | `'going_private'` |
| `form_type` | `string` | |
| `filed_date` | `date` | |
| `counterparty_cik` | `int64` | |
| `counterparty_name` | `string` | |
| `split_ratio_numerator` | `int32` | |
| `split_ratio_denominator` | `int32` | |
| `description` | `string` | |

---

## 9. Gold Layer — Parquet Schema

Gold Parquet files are rebuilt from Silver on every run. No incremental logic.

### 9.1 `gold/financial_statements_annual/refreshed_date=*/data.parquet`

Columns: `security_id`, `cik`, `ticker`, `company_name`, `exchange`, `fiscal_year`, `period_end`, `filed_date`, `taxonomy`, `revenues`, `net_income`, `operating_income`, `total_assets`, `total_liabilities`, `stockholders_equity`, `long_term_debt`, `cash_and_equivalents`, `operating_cash_flow`, `eps_basic`, `eps_diluted`, plus derived ratios:
- `net_margin` = `net_income / revenues`
- `return_on_equity` = `net_income / stockholders_equity`
- `debt_to_equity` = `long_term_debt / stockholders_equity`

### 9.2 `gold/financial_statements_quarterly/refreshed_date=*/data.parquet`

Same as annual plus `fiscal_period` (`'Q1'`–`'Q4'`). Most recent 8 quarters.

### 9.3 `gold/company_profile/refreshed_date=*/data.parquet`

One row per security. Columns: `security_id`, `cik`, `ticker`, `company_name`, `exchange`, `sic`, `sic_description`, `state_of_incorporation`, `fiscal_year_end`, `active_flag`, `inactive_reason`, `latest_10k_date`, `latest_10q_date`, `latest_revenues`, `latest_total_assets`, `latest_shares_outstanding`, `latest_eps_diluted`.

### 9.4 `gold/filing_catalog/refreshed_date=*/data.parquet`

All indexed filings. Columns: `security_id`, `cik`, `ticker`, `company_name`, `accession_number`, `form_type`, `filed_date`, `period_of_report`, `filing_url`, `primary_document`.

### 9.5 `gold/corporate_events/refreshed_date=*/data.parquet`

Corporate events enriched with ticker and company name. Columns: `security_id`, `cik`, `ticker`, `company_name`, `event_type`, `event_date`, `effective_date`, `form_type`, `filed_date`, `description`.

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

**Batch writer (shared) — uses fsspec for cloud-agnostic writes:**
```python
# scripts/ingest/_batch_writer.py
import pyarrow as pa, pyarrow.parquet as pq
from config.settings import CLOUD_PROVIDER, AWS_REGION, AZURE_ACCOUNT

def _get_filesystem():
    if CLOUD_PROVIDER == "aws":
        import s3fs
        return s3fs.S3FileSystem()         # uses boto3 credential chain (IAM role, env vars, ~/.aws)
    if CLOUD_PROVIDER == "azure":
        import adlfs
        from azure.identity import DefaultAzureCredential
        return adlfs.AzureBlobFileSystem(
            account_name=AZURE_ACCOUNT,
            credential=DefaultAzureCredential()   # Managed Identity, az login, env vars
        )
    raise ValueError(CLOUD_PROVIDER)

_FS = _get_filesystem()

def write_batch(records: list[dict], schema: pa.Schema,
                storage_root: str, dataset: str,
                ingest_date: str, batch_num: int) -> str:
    path = f"{storage_root}/bronze/{dataset}/ingestion_date={ingest_date}/batch_{batch_num:04d}.parquet"
    table = pa.Table.from_pylist(records, schema=schema)
    with _FS.open(path, "wb") as f:
        pq.write_table(table, f, compression="snappy")
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

### 11.4 `scripts/bronze_to_silver/01_silver_dim_security.py` — Security Master

DuckDB stateless transform. Reads today's bronze tickers Parquet from S3/Azure, writes silver `dim_security` Parquet back to S3/Azure. No local DB file.

```python
import duckdb, os
from config.settings import STORAGE_ROOT, CLOUD_PROVIDER, AWS_REGION, INGEST_DATE

conn = duckdb.connect()  # in-memory only — extensions pre-installed in Dockerfile

if CLOUD_PROVIDER == "aws":
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_region='{AWS_REGION}';")
    # Credentials come from ECS task IAM role via instance metadata — no env vars needed
elif CLOUD_PROVIDER == "azure":
    conn.execute("LOAD azure;")
    # AZURE_CLIENT_ID env var enables DefaultAzureCredential → Managed Identity

out_path = f"{STORAGE_ROOT}/silver/dim_security/snapshot_date={INGEST_DATE}/data.parquet"

conn.execute(f"""
  COPY (
    SELECT
      left(sha256(lpad(cast(cik as varchar), 10, '0') || '|' || 'PRIMARY'), 16) AS security_id,
      cik,
      ticker,
      NULL::VARCHAR                AS ticker_class,
      company_name,
      exchange,
      NULL::VARCHAR                AS sic,
      NULL::VARCHAR                AS entity_type,
      TRUE                         AS active_flag,
      '{INGEST_DATE}'::DATE        AS first_seen_date,
      '{INGEST_DATE}'::DATE        AS last_seen_date,
      current_timestamp            AS created_at,
      current_timestamp            AS updated_at
    FROM read_parquet('{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date={INGEST_DATE}/*.parquet')
    WHERE exchange IN ('NYSE', 'Nasdaq')
  ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
""")
print(f"Written dim_security → {out_path}")
conn.close()
```

### 11.5 `scripts/bronze_to_silver/02_silver_filings_and_facts.py`

DuckDB stateless transform. Reads today's bronze `submissions_raw` and `companyfacts_raw` Parquet from S3/Azure. Parses filings parallel arrays (JSON) and XBRL concepts. Writes silver `filings_index`, `financial_facts`, and `corporate_actions` Parquet to S3/Azure.

Key operations:
- Unnest `filings_recent_json` using DuckDB's `json_extract` and `unnest` to expand parallel arrays
- For each XBRL concept in `financial_facts`, apply period filtering (see Section 10) and pick primary/fallback mapping
- For `corporate_actions`, detect event types by `form_type` and `items` values (see Section 10)

### 11.6 `scripts/silver_to_gold/01_build_gold.py`

DuckDB stateless transform. Reads silver Parquet from S3/Azure, writes gold Parquet back to S3/Azure. Rebuilds all gold files from scratch on every run.

```python
# Pattern for each gold table:
conn.execute(f"""
  COPY (
    SELECT
      s.security_id, s.cik, s.ticker, s.company_name, s.exchange,
      f.fiscal_year, f.period_end, f.filed_date, f.taxonomy,
      f.revenues, f.net_income, f.operating_income,
      f.total_assets, f.total_liabilities, f.stockholders_equity,
      f.long_term_debt, f.cash_and_equivalents, f.operating_cash_flow,
      f.eps_basic, f.eps_diluted,
      f.net_income / NULLIF(f.revenues, 0)             AS net_margin,
      f.net_income / NULLIF(f.stockholders_equity, 0)  AS return_on_equity,
      f.long_term_debt / NULLIF(f.stockholders_equity, 0) AS debt_to_equity,
      current_timestamp AS refreshed_at
    FROM read_parquet('{STORAGE_ROOT}/silver/dim_security/snapshot_date={INGEST_DATE}/*.parquet') s
    JOIN read_parquet('{STORAGE_ROOT}/silver/financial_facts/snapshot_date={INGEST_DATE}/*.parquet') f
      ON s.security_id = f.security_id
    WHERE f.form_type IN ('10-K', '10-K/A')
  ) TO '{STORAGE_ROOT}/gold/financial_statements_annual/refreshed_date={INGEST_DATE}/data.parquet'
  (FORMAT PARQUET, COMPRESSION SNAPPY)
""")
```

---

## 12. Orchestration

All three environments (local, AWS, Azure) run the **same Docker container** with different `CMD` overrides per task. Credentials are injected via environment variables; no secrets are baked into the image.

**Pipeline execution order — tasks run sequentially:**
```
[ingest_tickers]          ← Stage 1: 1 HTTP call, writes data.parquet
         │
         ▼
[ingest_submissions]      ← Stage 2a: 8 req/s, writes batch_NNNN.parquet (~16 files)
         │
         ▼
[ingest_companyfacts]     ← Stage 2b: 8 req/s sequential (NOT parallel with Stage 2a)
         │                             Reason: combined 16 req/s would exceed SEC's 10 req/s limit
         ▼
[bronze_gate]             ← asserts Parquet file count > 0 for all 3 bronze datasets
         │
         ▼
[silver_dim_security]     ← DuckDB in-memory: bronze tickers → silver/dim_security/
         │
         ▼
[silver_filings_facts]    ← DuckDB in-memory: bronze submissions+facts → silver/filings_index/, financial_facts/, corporate_actions/
         │
         ▼
[build_gold]              ← DuckDB in-memory: silver → gold/ (all 5 tables)
```

**Rule:** All bronze tasks must complete before any silver task starts. The `bronze_gate` task enforces this.

### 12.1 Docker Container (all environments)

```dockerfile
# Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Pre-install DuckDB extensions — containers in private VPCs have no internet egress
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs; INSTALL azure;')"
COPY . .
ENTRYPOINT ["python"]
CMD ["-m", "pipeline"]
```

```
# requirements.txt
requests>=2.31
pyarrow>=14.0
s3fs>=2024.2.0
adlfs>=2024.2.0
azure-identity>=1.15
duckdb>=1.0.0
```

Push to registry:
- **AWS**: `docker build -t {account}.dkr.ecr.{region}.amazonaws.com/sec-edgar-ingest:latest . && docker push ...`
- **Azure**: `docker build -t {registry}.azurecr.io/sec-edgar-ingest:latest . && docker push ...`

### 12.2 Option A — Azure Data Factory (ADF)

**Infrastructure requirements:**
| Component | Requirement |
|---|---|
| Storage account | ADLS Gen2 (Hierarchical Namespace **must** be enabled) |
| Azure Batch pool | Ubuntu 22.04 + `containerConfiguration` enabled; pool identity = User-Assigned Managed Identity |
| ACR | Stores Docker image; Batch pool identity has `AcrPull` role |
| Managed Identity | `Storage Blob Data Contributor` on the ADLS Gen2 storage account |

**ADF pipeline** (`workflows/adf_pipeline.json`): Custom Activity per task, all on Azure Batch. Dependencies expressed as ADF activity dependencies.

**Passing parameters to container**: Use CLI args in the `command` field (not `extendedProperties`):
```json
{
  "name": "ingest_tickers",
  "type": "Custom",
  "linkedServiceName": { "referenceName": "AzureBatchLS" },
  "typeProperties": {
    "command": "scripts/ingest/01_ingest_tickers_exchange.py --date @{pipeline().parameters.ingestDate}",
    "resourceLinkedService": { "referenceName": "AzureStorageLS" },
    "folderPath": "sec-edgar-adf-scripts"
  }
}
```

**Trigger**: Tumbling Window Trigger (not Schedule Trigger) — guarantees exactly-once execution per 24h window, supports backfill of missed windows.

Files to create: `workflows/adf_pipeline.json`, `workflows/adf_linked_services.json`, `workflows/adf_trigger.json`

### 12.3 Option B — AWS Step Functions + ECS Fargate

**Infrastructure requirements:**
| Component | Requirement |
|---|---|
| S3 bucket | Standard; versioning optional |
| ECS cluster | Fargate launch type |
| VPC | Private subnets + NAT Gateway (for SEC API outbound; ~$0.045/hr) |
| ECS task role | `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `{bucket}/{prefix}/*` |
| ECR | Stores Docker image; task execution role has ECR pull permission |

**Step Functions state machine** (`workflows/step_functions_definition.json`): each state runs a Fargate task with a CMD override. `NetworkConfiguration` is required for Fargate.

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::ecs:runTask.sync",
  "Parameters": {
    "Cluster": "arn:aws:ecs:{region}:{account}:cluster/sec-edgar-cluster",
    "TaskDefinition": "arn:aws:ecs:{region}:{account}:task-definition/sec-edgar-ingest",
    "LaunchType": "FARGATE",
    "NetworkConfiguration": {
      "AwsvpcConfiguration": {
        "Subnets.$":        "$.subnets",
        "SecurityGroups.$": "$.securityGroups",
        "AssignPublicIp":   "DISABLED"
      }
    },
    "Overrides": {
      "ContainerOverrides": [{
        "Name": "sec-edgar",
        "Command.$": "States.Array('scripts/ingest/01_ingest_tickers_exchange.py', '--date', $.ingestDate)",
        "Environment": [
          { "Name": "CLOUD_PROVIDER", "Value": "aws" }
        ]
      }]
    }
  },
  "Retry": [{ "ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 30, "MaxAttempts": 2 }]
}
```

**Trigger**: EventBridge Scheduler at 06:00 UTC daily, passing today's date as `$.ingestDate`.

Files to create: `workflows/step_functions_definition.json`, `workflows/ecs_task_definition.json`, `workflows/iam_task_role_policy.json`

### 12.4 Option C — Local / Dev (`pipeline.py`)

```python
# pipeline.py — sequential local runner
import subprocess, concurrent.futures
from config.settings import INGEST_DATE

def run(args): subprocess.run(["python"] + args, check=True)

# Stage 1
run(["scripts/ingest/01_ingest_tickers_exchange.py", "--date", INGEST_DATE])
# Stage 2 — sequential (not parallel, to respect SEC rate limit)
run(["scripts/ingest/02_ingest_submissions.py",  "--date", INGEST_DATE])
run(["scripts/ingest/03_ingest_companyfacts.py", "--date", INGEST_DATE])
# Bronze gate
run(["scripts/ingest/bronze_gate.py"])
# Silver
run(["scripts/bronze_to_silver/01_silver_dim_security.py"])
run(["scripts/bronze_to_silver/02_silver_filings_and_facts.py"])
# Gold
run(["scripts/silver_to_gold/01_build_gold.py"])
```

---

## 13. Project File Layout

```
sec_edgar_platform/
├── Dockerfile                              ← single image for ADF Batch, ECS Fargate, and local
├── requirements.txt                        ← pyarrow, s3fs, adlfs, azure-identity, duckdb, requests
├── pipeline.py                             ← local/dev sequential runner (Option C)
├── config/
│   └── settings.py                         ← CLOUD_PROVIDER, storage config (all from env vars)
├── scripts/
│   ├── ingest/
│   │   ├── _rate_limiter.py                ← thread-safe RateLimiter (8 req/s per task)
│   │   ├── _http.py                        ← edgar_get() with retry + rate limit
│   │   ├── _batch_writer.py                ← write_batch() via fsspec (s3fs or adlfs)
│   │   ├── 01_ingest_tickers_exchange.py   ← 1 HTTP call → data.parquet in S3/Azure
│   │   ├── 02_ingest_submissions.py        ← 8 workers → batch_NNNN.parquet in S3/Azure
│   │   ├── 03_ingest_companyfacts.py       ← 8 workers → batch_NNNN.parquet in S3/Azure
│   │   └── bronze_gate.py                  ← asserts Parquet file count > 0 (raises on failure)
│   ├── bronze_to_silver/
│   │   ├── 01_silver_dim_security.py       ← DuckDB in-memory: bronze → silver/dim_security/
│   │   └── 02_silver_filings_and_facts.py  ← DuckDB in-memory: bronze → silver/filings_index/ + financial_facts/ + corporate_actions/
│   └── silver_to_gold/
│       └── 01_build_gold.py                ← DuckDB in-memory: silver → gold/ (5 tables)
├── workflows/
│   ├── adf_pipeline.json                   ← Azure: ADF Custom Activity pipeline
│   ├── adf_linked_services.json            ← Azure: Batch + Storage linked service definitions
│   ├── adf_trigger.json                    ← Azure: Tumbling Window Trigger (daily 06:00 UTC)
│   ├── step_functions_definition.json      ← AWS: Step Functions state machine ASL
│   ├── ecs_task_definition.json            ← AWS: ECS task definition (taskRoleArn, awsvpc, resources)
│   └── iam_task_role_policy.json           ← AWS: S3 read/write IAM policy
└── tests/
    └── test_spot_check.py                  ← DuckDB queries over S3/Azure Parquet for verification
```

**No `scripts/setup/` SQL files** — there is no database to initialize. Parquet directories are created automatically by the first write.

---

## 14. Verification Steps

All verification uses DuckDB reading Parquet directly from S3/Azure — no database connection needed.

### Step 1 — Storage access
```bash
# AWS
aws s3 ls s3://{BUCKET}/{PREFIX}/
# Expected: no error

# Azure
az storage blob list --container-name sec-edgar --account-name myaccount --prefix sec-edgar/
```

### Step 2 — Smoke test ingestion (local, 5 CIKs)
```bash
export CLOUD_PROVIDER=aws   # or azure
export AWS_BUCKET=my-bucket  # or AZURE_STORAGE_ACCOUNT / AZURE_CONTAINER

python scripts/ingest/01_ingest_tickers_exchange.py
# Expected: data.parquet written to {STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date=<today>/

python scripts/ingest/02_ingest_submissions.py --date today --limit 5
python scripts/ingest/03_ingest_companyfacts.py --date today --limit 5
```

### Step 3 — Verify bronze Parquet
```python
import duckdb
conn = duckdb.connect()
conn.execute("LOAD httpfs; SET s3_region='us-east-1';")  # or LOAD azure;

# Tickers
print(conn.execute("""
  SELECT COUNT(*), MIN(ingestion_date), MAX(ingestion_date)
  FROM read_parquet('s3://my-bucket/sec-edgar/bronze/company_tickers_exchange/*/*.parquet',
                    hive_partitioning=true)
""").fetchone())
# Expected: (10000+, 'today', 'today')
```

### Step 4 — Silver security master
```python
print(conn.execute("""
  SELECT COUNT(*) AS total,
         SUM(CASE WHEN active_flag THEN 1 ELSE 0 END) AS active
  FROM read_parquet('s3://my-bucket/sec-edgar/silver/dim_security/*/*.parquet',
                    hive_partitioning=true)
""").fetchone())
# Expected: (6000+, 6000+) — NYSE + Nasdaq only
```

### Step 5 — Spot check known tickers
```python
print(conn.execute("""
  SELECT s.ticker, s.company_name, f.period_end, f.revenues, f.net_income
  FROM read_parquet('.../silver/dim_security/*/*.parquet') s
  JOIN read_parquet('.../silver/financial_facts/*/*.parquet') f
    ON s.security_id = f.security_id
  WHERE s.ticker IN ('AAPL', 'MSFT', 'TSLA', 'JPM')
    AND f.form_type = '10-K'
  ORDER BY s.ticker, f.period_end DESC
""").df())
```

### Step 6 — Gold tables ready
```python
print(conn.execute("""
  SELECT ticker, fiscal_year, revenues, net_margin, return_on_equity
  FROM read_parquet('.../gold/financial_statements_annual/*/*.parquet')
  WHERE ticker IN ('AAPL', 'MSFT')
  ORDER BY ticker, fiscal_year DESC
""").df())
```

### Step 7 — Full pipeline run
```bash
# Local
python pipeline.py

# AWS (trigger Step Functions manually)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:{region}:{account}:stateMachine:sec-edgar-daily \
  --input '{"ingestDate": "2024-01-25", "subnets": ["subnet-xxx"], "securityGroups": ["sg-xxx"]}'

# Azure (trigger ADF pipeline manually)
az datafactory pipeline create-run \
  --factory-name my-adf --resource-group my-rg \
  --pipeline-name sec_edgar_daily \
  --parameters '{"ingestDate": "2024-01-25", "storageAccount": "myaccount"}'
# Expected runtime: ~60–90 min for full initial load of ~8,000 companies
```

### Step 8 — Idempotency check
Re-run the pipeline for the same date. Silver/Gold Parquet files are overwritten with identical content (same row counts). Bronze skips files that already exist.

---

## 15. Edge Cases and Error Handling

| Scenario | Handling |
|---|---|
| Company uses IFRS (not us-gaap) | Try us-gaap facts first, then ifrs-full; set `taxonomy = 'ifrs-full'` |
| `companyfacts` returns 404 | Log CIK to checkpoint; write NULL financial facts; do not retry on same day |
| Multi-class shares (BRK.A/BRK.B) | Both share same CIK; `ticker_class` differentiates; both get separate `security_id` |
| HTTP 429 rate limit | Sleep 60s, retry up to 3× with exponential backoff |
| `submissions.filings.files` pagination | Fetch continuation pages, cap at 5 pages per CIK |
| SEC rate limit across tasks | Ingest tasks run **sequentially** — each uses ≤8 req/s; never exceed 10 req/s total |
| DuckDB extensions in VPC | Extensions are pre-installed in the Dockerfile — no runtime internet download needed |
| DuckDB S3 auth (ECS) | Credentials come from ECS task IAM role via instance metadata — no env vars needed |
| DuckDB Azure auth (Batch) | Set `AZURE_CLIENT_ID` env var; `DefaultAzureCredential` picks up Managed Identity |
| SEC site maintenance (weekends) | Pipeline is idempotent — next run rewrites today's Silver/Gold Parquet |
| Large companyfacts payload (>10 MB) | Store as-is in Parquet STRING column; DuckDB parses JSON in Silver transform |
| ADF Batch pool node unavailable | ADF retries automatically; Batch scales pool nodes up/down |
| ECS Fargate task fails | Step Functions retry policy (`MaxAttempts: 2`) automatically retries failed states |

---

## 16. How to Start Coding (Post-Review Checklist)

Once you have reviewed this spec and are ready to implement, follow these steps in order. Each step is independently testable.

### Phase 0 — Environment Setup (Do Once)

- [ ] **Choose cloud**: set `CLOUD_PROVIDER=aws` or `CLOUD_PROVIDER=azure` as an env var.
- [ ] **Provision storage** (Section 5): create S3 bucket or ADLS Gen2 storage account + container.
- [ ] **Configure credentials** locally:
  - AWS: `aws configure` or set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` + `AWS_DEFAULT_REGION`
  - Azure: `az login` (DefaultAzureCredential picks it up) + set `AZURE_STORAGE_ACCOUNT` / `AZURE_CONTAINER`
- [ ] **Create Python virtual environment**:
  ```bash
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  ```
- [ ] **No database to set up.** There is no DDL to run. Parquet directories are created automatically.

### Phase 1 — Create the Project Scaffold

```bash
mkdir -p config scripts/{ingest,bronze_to_silver,silver_to_gold} workflows tests
touch config/__init__.py config/settings.py
```

Copy the settings template from **Section 4** into `config/settings.py` and fill in your values.

### Phase 2 — Write and Test the Ingestion Scripts

1. **`scripts/ingest/_rate_limiter.py`** — Section 11.0
2. **`scripts/ingest/_http.py`** — Section 11.0
3. **`scripts/ingest/_batch_writer.py`** — Section 11.0 (updated with fsspec/s3fs/adlfs)
4. **`scripts/ingest/01_ingest_tickers_exchange.py`** — Section 11.1
   - Test: `INGEST_DATE=2024-01-25 python scripts/ingest/01_ingest_tickers_exchange.py`
   - Verify: Parquet appears in `{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date=2024-01-25/`
   - Check row count ≈ 10,000–12,000
5. **`scripts/ingest/02_ingest_submissions.py`** — Section 11.2
   - Dev test: `--limit 5` flag
6. **`scripts/ingest/03_ingest_companyfacts.py`** — Section 11.3
   - Dev test: `--limit 5`; AAPL (CIK 320193) is a good stress test for large payload

### Phase 3 — Write Silver Transforms (DuckDB)

1. **`scripts/bronze_to_silver/01_silver_dim_security.py`** — Section 11.4
   - The `security_id` hash is the most critical piece — test it with known CIKs first (Section 8.1)
   - Verify: DuckDB `SELECT COUNT(*) FROM read_parquet('.../silver/dim_security/.../data.parquet')`

2. **`scripts/bronze_to_silver/02_silver_filings_and_facts.py`** — Section 11.5
   - Start with `filings_index` (simpler JSON parsing), then add `financial_facts`
   - Key complexity: unnesting parallel arrays from `filings_recent_json` and XBRL concept extraction

### Phase 4 — Write Gold Transforms (DuckDB)

**`scripts/silver_to_gold/01_build_gold.py`** — Section 11.6

Start with `financial_statements_annual` (highest value). Each gold table is a single DuckDB `COPY ... TO` with a `SELECT ... JOIN` of silver Parquet.

### Phase 5 — Wire Up Cloud Orchestration

**Azure:** Create `workflows/adf_pipeline.json` (Section 12.2). Deploy via ADF portal or ARM deployment.

**AWS:** Create `workflows/step_functions_definition.json` and `workflows/ecs_task_definition.json` (Section 12.3). Deploy via `aws cloudformation` or Terraform.

### Phase 6 — Full Verification

Run all verification steps from **Section 14** in order.

---

### Implementation Priority (deliver value incrementally)

| Priority | Deliverable | Sections |
|---|---|---|
| 1 (highest) | Bronze tickers + silver `dim_security` | 6, 7.1, 8.1, 11.1–11.4 |
| 2 | Bronze submissions + silver `filings_index` | 7.2, 8.2, 11.2 |
| 3 | Bronze companyfacts + silver `financial_facts` | 7.3, 8.3, 11.3, 11.5 |
| 4 | Gold annual financials | 9.1, 11.6 |
| 5 | Gold quarterly + company profile + corporate events | 9.2–9.5 |
| 6 | Cloud orchestration (ADF or Step Functions) | 12 |

**Start with Priority 1.** You'll have a queryable security master with `security_id` before writing a single financial fact row.
