"""
Pipeline configuration — reads from environment variables.

Required env vars:
  SEC_USER_AGENT      "YourOrg Pipeline you@yourorg.com"
                      Format mandated by SEC Developer Resources.
                      KeyError at import time if missing.

Optional env vars (Phase 2 — Azure ingest):
  CLOUD_PROVIDER      "azure" | "aws" | "local"   (default: "local")
  AZURE_STORAGE_ACCOUNT  e.g. "mysecedgarstorage"
  AZURE_CONTAINER        e.g. "sec-edgar"           (default: "sec-edgar")
  AWS_BUCKET             e.g. "my-sec-edgar-bucket"
  STORAGE_PREFIX         e.g. "sec-edgar"           (default: "sec-edgar")
  INGEST_DATE            YYYY-MM-DD  (default: today)
"""
import os
from datetime import date

# ── SEC EDGAR API (REQUIRED) ──────────────────────────────────────────────────
# SEC mandates a User-Agent header with app name + contact email on every request.
# KeyError here is intentional — prevents silent non-compliant HTTP calls.
USER_AGENT = os.environ["SEC_USER_AGENT"]

MAX_RETRIES     = 3
REQUEST_TIMEOUT = 20   # seconds per request

# ── Ingestion tuning ─────────────────────────────────────────────────────────
INGEST_RATE_RPS  = 8.0   # req/s — 20% below SEC's 10 req/s hard limit
INGEST_WORKERS   = 4     # ThreadPoolExecutor workers for submissions fetch
BATCH_SIZE       = 500   # CIKs per Parquet file
TARGET_EXCHANGES = ["NYSE", "Nasdaq"]

# ── Ingest date (injectable for backfill) ─────────────────────────────────────
INGEST_DATE: str = os.environ.get("INGEST_DATE", date.today().isoformat())

# ── Cloud provider ─────────────────────────────────────────────────────────────
CLOUD_PROVIDER: str = os.environ.get("CLOUD_PROVIDER", "local")  # "local" | "azure" | "aws"

# ── AWS ───────────────────────────────────────────────────────────────────────
AWS_BUCKET    = os.environ.get("AWS_BUCKET", "")
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# ── Azure ─────────────────────────────────────────────────────────────────────
AZURE_ACCOUNT    = os.environ.get("AZURE_STORAGE_ACCOUNT", "")
AZURE_CONTAINER  = os.environ.get("AZURE_CONTAINER", "sec-edgar")

# ── Storage prefix (shared across clouds) ─────────────────────────────────────
STORAGE_PREFIX = os.environ.get("STORAGE_PREFIX", "sec-edgar")


def storage_root() -> str:
    """
    Return the root storage path for the active cloud provider.

    local  ->  output/
    azure  ->  abfss://{container}@{account}.dfs.core.windows.net/{prefix}
    aws    ->  s3://{bucket}/{prefix}
    """
    if CLOUD_PROVIDER == "local":
        return "output"
    if CLOUD_PROVIDER == "azure":
        if not AZURE_ACCOUNT:
            raise ValueError("AZURE_STORAGE_ACCOUNT env var is required for CLOUD_PROVIDER=azure")
        return f"abfss://{AZURE_CONTAINER}@{AZURE_ACCOUNT}.dfs.core.windows.net/{STORAGE_PREFIX}"
    if CLOUD_PROVIDER == "aws":
        if not AWS_BUCKET:
            raise ValueError("AWS_BUCKET env var is required for CLOUD_PROVIDER=aws")
        return f"s3://{AWS_BUCKET}/{STORAGE_PREFIX}"
    raise ValueError(f"Unknown CLOUD_PROVIDER: {CLOUD_PROVIDER!r}  (expected: local | azure | aws)")


STORAGE_ROOT: str = storage_root()
