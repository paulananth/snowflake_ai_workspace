# SEC EDGAR Security & Financial Data Platform — SPECIFICATION.md

**Platform:** Cloud-agnostic (AWS or Azure)  **Architecture:** Medallion (Bronze / Silver / Gold)  **Source:** SEC EDGAR only

> **Before you start:** Two Silver/Gold destination paths are supported. Choose one and follow only those sections.
>
> | | **Path A — DuckDB (Parquet)** | **Path B — Snowflake** |
> |---|---|---|
> | Silver/Gold storage | Parquet files in S3 / Azure Blob | Snowflake tables |
> | Transform tool | DuckDB (stateless, in-memory) | dbt-snowflake |
> | Bronze loading | Not needed — DuckDB reads Parquet directly | COPY INTO from external stage |
> | Query interface | DuckDB CLI / Python `read_parquet()` | Snowflake UI, SnowSQL, any BI tool |
> | Best for | Minimal dependencies, cost-sensitive | Teams already on Snowflake, need dbt testing/docs |
>
> **Bronze is identical in both paths** — always Parquet in S3 / Azure Blob.

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

### Path A — DuckDB (Parquet in S3 / Azure Blob)

```
SEC EDGAR APIs
     │
     ▼ HTTP → Parquet (pyarrow + s3fs / adlfs)
Object Storage
  bronze/   ← append-only raw Parquet
  silver/   ← Parquet written by DuckDB transforms
  gold/     ← Parquet written by DuckDB transforms
     │
     ▼ DuckDB (in-memory per container task)
  reads Parquet from S3/Azure → SQL transform in RAM → writes Parquet to S3/Azure
```

### Path B — Snowflake

```
SEC EDGAR APIs
     │
     ▼ HTTP → Parquet (pyarrow + s3fs / adlfs)
Object Storage
  bronze/   ← append-only raw Parquet  (same as Path A)
     │
     ▼ COPY INTO via Snowflake External Stage
Snowflake
  {DB}.bronze.*   ← raw tables loaded from S3/Azure stage
     │
     ▼ dbt-snowflake (incremental models)
  {DB}.silver.*   ← parsed, validated, keyed by security_id
     │
     ▼ dbt-snowflake (table models)
  {DB}.gold.*     ← analytics-ready joins + computed ratios
```

**Shared design principles (both paths):**
- Bronze is **always Parquet in S3/Azure** — append-only, never modified, full audit trail
- `security_id` is a deterministic 16-char hex hash — stable across re-runs, no sequences needed
- Ingest tasks run **sequentially** to stay under SEC's 10 req/s total rate limit
- Single Docker image handles ingestion on both paths; Silver/Gold compute differs per path

---

## 3. Prerequisites

**AWS:**
1. **S3 bucket** in your target region (e.g. `my-sec-edgar-bucket`)
2. **IAM roles**: ECS Task Role, ECS Execution Role, Step Functions Role, EventBridge Scheduler Role — see **Section 6.1**
3. **ECR repository** to store the Docker image
4. **ECS cluster** (Fargate launch type) + VPC with private subnets and a NAT Gateway (for SEC API outbound)
5. **AWS Step Functions** state machine (see Section 13)

**Azure:**
1. **Storage account** with **Hierarchical Namespace enabled** (ADLS Gen2) — required for `abfss://` scheme
2. **User-Assigned Managed Identity** with RBAC roles — see **Section 6.2**
3. **Azure Container Registry (ACR)** to store the Docker image
4. **Azure Batch account** with a pool configured for Docker container execution; pool identity = Managed Identity above
5. **Azure Data Factory** pipeline (see Section 13)

**Both clouds (local dev):**
- **Python 3.11+** and **Docker** installed locally
- **Cloud CLI** (`aws` or `az`) configured — see **Section 6.1.5** (AWS) or **Section 6.2.6** (Azure)

**Path B (Snowflake) — additional prerequisites:**
- **Snowflake account** (any edition; Enterprise recommended for `MERGE` performance)
- **Snowflake database** created: `CREATE DATABASE sec_edgar;`
- **Snowflake Storage Integration** connecting Snowflake to S3 or ADLS Gen2 — see **Section 6.4**
- **dbt CLI** installed: `pip install dbt-snowflake`
- **Key-pair authentication** configured on the Snowflake service user — see **Section 6.4**

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
# Auth: see Section 6.1 (ECS task IAM role in production; ~/.aws credentials locally)

# ─── Azure ─────────────────────────────────────────────────────────────────────
AZURE_ACCOUNT   = os.environ.get("AZURE_STORAGE_ACCOUNT", "myaccount")   # CHANGE THIS
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER",       "sec-edgar")   # CHANGE THIS
# Requires ADLS Gen2 (Hierarchical Namespace enabled on the storage account)
# Auth: see Section 6.2 (User-Assigned Managed Identity; AZURE_CLIENT_ID env var on Batch pool)

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

## 6. Authentication & Permissions

Authentication follows **zero-secrets-in-code** principles: containers get credentials from the compute identity (ECS task IAM role on AWS, Managed Identity on Azure). No access keys, storage account keys, or service principal secrets are stored in Docker images, config files, or environment variables on production systems.

---

### 6.1 AWS — Identity & Access Model

#### Principal hierarchy

```
EventBridge Scheduler
  └─ assumes → Step Functions Execution Role
                 └─ calls ecs:RunTask → ECS Task
                              └─ assumes → ECS Task Role  ← Python code runs as this
```

#### 6.1.1 ECS Task Role (what the container code is)

Attached to the ECS task definition as `taskRoleArn`. Python scripts inside the container inherit it automatically via the EC2 instance metadata endpoint — no env vars needed.

**Trust policy** (`workflows/iam/ecs_task_trust.json`):
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ecs-tasks.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

**Permissions policy** (`workflows/iam/ecs_task_role_policy.json`):
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "S3ReadWrite",
    "Effect": "Allow",
    "Action": [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:ListBucket", "s3:GetBucketLocation"
    ],
    "Resource": [
      "arn:aws:s3:::{BUCKET}",
      "arn:aws:s3:::{BUCKET}/{STORAGE_PREFIX}/*"
    ]
  }]
}
```

```bash
aws iam create-role \
  --role-name sec-edgar-ecs-task-role \
  --assume-role-policy-document file://workflows/iam/ecs_task_trust.json

aws iam put-role-policy \
  --role-name sec-edgar-ecs-task-role \
  --policy-name S3ReadWrite \
  --policy-document file://workflows/iam/ecs_task_role_policy.json
```

#### 6.1.2 ECS Task Execution Role (ECS control-plane — pull image + write logs)

Attach the AWS-managed policy; no custom policy needed.

```bash
aws iam create-role \
  --role-name sec-edgar-ecs-execution-role \
  --assume-role-policy-document file://workflows/iam/ecs_task_trust.json

aws iam attach-role-policy \
  --role-name sec-edgar-ecs-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
```

#### 6.1.3 Step Functions State Machine Role

Allows the state machine to submit ECS tasks and pass the task/execution roles.

**Permissions policy** (`workflows/iam/sfn_role_policy.json`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RunECSTask",
      "Effect": "Allow",
      "Action": ["ecs:RunTask", "ecs:StopTask", "ecs:DescribeTasks"],
      "Resource": "*"
    },
    {
      "Sid": "PassTaskRoles",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::{ACCOUNT}:role/sec-edgar-ecs-task-role",
        "arn:aws:iam::{ACCOUNT}:role/sec-edgar-ecs-execution-role"
      ]
    },
    {
      "Sid": "EventBridgeSync",
      "Effect": "Allow",
      "Action": ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
      "Resource": "arn:aws:events:{REGION}:{ACCOUNT}:rule/StepFunctionsGetEventsForECSTaskRule"
    }
  ]
}
```

#### 6.1.4 EventBridge Scheduler Role

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "states:StartExecution",
    "Resource": "arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:sec-edgar-daily"
  }]
}
```

#### 6.1.5 Local Dev (AWS)

`s3fs.S3FileSystem()` and DuckDB `LOAD httpfs` use the standard boto3 credential chain: env vars → `~/.aws/credentials` → instance profile → ECS task role.

```bash
# Option A — named profile
aws configure --profile sec-edgar
export AWS_PROFILE=sec-edgar

# Option B — environment variables (CI/CD)
export AWS_ACCESS_KEY_ID=AKIAxxx
export AWS_SECRET_ACCESS_KEY=xxx
export AWS_DEFAULT_REGION=us-east-1
```

---

### 6.2 Azure — Identity & Access Model

#### Principal hierarchy

```
ADF Pipeline (system-assigned MSI)
  └─ Linked Service → Azure Batch account
       └─ Batch Pool (User-Assigned Managed Identity) ← containers run as this
            RBAC assignments on the pool identity:
              ├─ Storage Blob Data Contributor  →  ADLS Gen2 storage account
              └─ AcrPull                        →  Azure Container Registry
```

#### 6.2.1 Create the User-Assigned Managed Identity

```bash
az identity create \
  --name sec-edgar-ingest-identity \
  --resource-group my-rg \
  --location eastus

# Save the client ID — required as AZURE_CLIENT_ID env var on the Batch pool
CLIENT_ID=$(az identity show \
  --name sec-edgar-ingest-identity --resource-group my-rg \
  --query clientId --output tsv)

PRINCIPAL_ID=$(az identity show \
  --name sec-edgar-ingest-identity --resource-group my-rg \
  --query principalId --output tsv)
```

#### 6.2.2 RBAC Role Assignments

```bash
STORAGE_SCOPE="/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.Storage/storageAccounts/myaccount"
ACR_SCOPE="/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.ContainerRegistry/registries/myregistry"

# Read + write Parquet files in ADLS Gen2
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --scope $STORAGE_SCOPE

# Pull Docker image onto Batch pool nodes
az role assignment create \
  --role "AcrPull" \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --scope $ACR_SCOPE
```

**Minimum required roles — do not use broader roles:**

| Resource | Role | Justification |
|---|---|---|
| ADLS Gen2 storage account | `Storage Blob Data Contributor` | Read + write Parquet; does not grant account management |
| Azure Container Registry | `AcrPull` | Pull image only; does not allow push or admin |
| Azure Batch account (ADF linked service) | `Contributor` scoped to Batch account | ADF submits jobs; cannot access storage or other resources |

#### 6.2.3 Assign Identity to Azure Batch Pool

```bash
IDENTITY_ID="/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/sec-edgar-ingest-identity"

az batch pool create \
  --id sec-edgar-pool \
  --account-name mybatchaccount \
  --vm-size Standard_D4s_v3 \
  --target-dedicated-nodes 2 \
  --image canonical:0001-com-ubuntu-server-jammy:22_04-lts \
  --node-agent-sku-id "batch.node.ubuntu 22.04" \
  --identity $IDENTITY_ID
```

#### 6.2.4 Container Environment Variable

When multiple managed identities could be present on a node, `DefaultAzureCredential` needs a hint. Set this on the Batch pool's environment settings (not in the Docker image):

```
AZURE_CLIENT_ID = <client-id from 6.2.1>
```

`adlfs.AzureBlobFileSystem(credential=DefaultAzureCredential())` and DuckDB `LOAD azure` both pick it up automatically.

#### 6.2.5 ADF System Identity Permissions

ADF connects to Batch and Storage using its own system-assigned managed identity:

```bash
ADF_PRINCIPAL=$(az datafactory show \
  --name my-adf --resource-group my-rg \
  --query identity.principalId --output tsv)

BATCH_SCOPE="/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.Batch/batchAccounts/mybatchaccount"

az role assignment create --role "Contributor" \
  --assignee-object-id $ADF_PRINCIPAL \
  --scope $BATCH_SCOPE

az role assignment create --role "Storage Blob Data Reader" \
  --assignee-object-id $ADF_PRINCIPAL \
  --scope $STORAGE_SCOPE
```

#### 6.2.6 Local Dev (Azure)

`DefaultAzureCredential` automatically picks up `az login`:

```bash
az login
az account set --subscription {SUB}

# Verify access
az storage blob list --container-name sec-edgar \
  --account-name myaccount --auth-mode login
```

For CI/CD or non-interactive environments:
```bash
export AZURE_CLIENT_ID=xxx
export AZURE_CLIENT_SECRET=xxx
export AZURE_TENANT_ID=xxx
```

---

### 6.3 What NOT To Do

| Anti-pattern | Correct approach |
|---|---|
| AWS access keys in `.env`, config, or Docker image | ECS task IAM role via instance metadata — zero env vars |
| `AZURE_STORAGE_ACCOUNT_KEY` anywhere | Managed Identity + `DefaultAzureCredential` |
| Broad policies (`s3:*`, `Storage Account Contributor`) | Scope to minimum actions on specific resource ARN/ID |
| Hardcoded credentials in `config/settings.py` | All credentials come from the runtime compute identity |
| One IAM role/identity shared across dev/staging/prod | Separate identity per environment with separate S3 prefix |
| Baking `AZURE_CLIENT_SECRET` into the Docker image | Set only `AZURE_CLIENT_ID` on the Batch pool; secret auth is for service principals, not managed identities |

---

### 6.4 Path B — Snowflake Authentication & Storage Integration

#### 6.4.1 Key-Pair Authentication (required for non-interactive/service use)

```bash
# Generate key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Register public key on the Snowflake service user
snowsql -q "ALTER USER transformer_svc SET RSA_PUBLIC_KEY='$(grep -v 'PUBLIC KEY' rsa_key.pub | tr -d '\n')';"
```

Store `rsa_key.p8` in **AWS Secrets Manager** or **Azure Key Vault** — never in the repo or Docker image. Inject at runtime as `SNOWFLAKE_PRIVATE_KEY_PATH` env var.

#### 6.4.2 Snowflake Storage Integration (connecting Snowflake to Bronze Parquet)

**AWS:**
```sql
-- Run as ACCOUNTADMIN
CREATE STORAGE INTEGRATION sec_edgar_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{ACCOUNT}:role/snowflake-s3-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://{BUCKET}/{PREFIX}/bronze/');

-- Snowflake gives you an IAM user to trust:
DESC INTEGRATION sec_edgar_s3_int;
-- Note STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Add a trust relationship to the IAM role for that user
```

**Azure:**
```sql
CREATE STORAGE INTEGRATION sec_edgar_adls_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '{TENANT_ID}'
  STORAGE_ALLOWED_LOCATIONS = ('azure://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/{PREFIX}/bronze/');

DESC INTEGRATION sec_edgar_adls_int;
-- Note AZURE_CONSENT_URL — open it to grant Snowflake access to the storage account
```

#### 6.4.3 External Stage (per dataset)

```sql
-- Run as SYSADMIN
CREATE DATABASE IF NOT EXISTS sec_edgar;
CREATE SCHEMA IF NOT EXISTS sec_edgar.bronze;
CREATE SCHEMA IF NOT EXISTS sec_edgar.silver;
CREATE SCHEMA IF NOT EXISTS sec_edgar.gold;

-- AWS stage
CREATE STAGE sec_edgar.bronze.s3_stage
  STORAGE_INTEGRATION = sec_edgar_s3_int
  URL = 's3://{BUCKET}/{PREFIX}/bronze/'
  FILE_FORMAT = (TYPE = PARQUET);

-- Azure stage
CREATE STAGE sec_edgar.bronze.adls_stage
  STORAGE_INTEGRATION = sec_edgar_adls_int
  URL = 'azure://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/{PREFIX}/bronze/'
  FILE_FORMAT = (TYPE = PARQUET);
```

#### 6.4.4 Snowflake Role & Warehouse

```sql
-- Run as USERADMIN / SYSADMIN
CREATE ROLE transformer_role;
CREATE WAREHOUSE transform_wh
  WAREHOUSE_SIZE = 'X-SMALL'   -- sufficient for 8,000 companies
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

GRANT USAGE ON WAREHOUSE transform_wh     TO ROLE transformer_role;
GRANT ALL    ON DATABASE  sec_edgar       TO ROLE transformer_role;
GRANT ALL    ON ALL SCHEMAS IN DATABASE sec_edgar TO ROLE transformer_role;
GRANT USAGE  ON INTEGRATION sec_edgar_s3_int  TO ROLE transformer_role;  -- or adls_int

CREATE USER transformer_svc DEFAULT_ROLE = transformer_role;
GRANT ROLE transformer_role TO USER transformer_svc;
```

#### 6.4.5 dbt Profile

```yaml
# ~/.dbt/profiles.yml
sec_edgar:
  target: prod
  outputs:
    prod:
      type: snowflake
      account:          "{{ env_var('SNOWFLAKE_ACCOUNT') }}"       # e.g. orgname-accountname
      user:             "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role:             transformer_role
      warehouse:        transform_wh
      database:         sec_edgar
      schema:           silver
      threads:          4
```

---

## 7. Object Storage Layout (All Three Layers)

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

## 8. Bronze Layer — Parquet Schema

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

## 9. Silver Layer

> **Path A (DuckDB):** Silver is Parquet files in S3/Azure Blob. Schema defined by PyArrow output.
> **Path B (Snowflake):** Silver is Snowflake tables in `sec_edgar.silver.*`. Schema defined by DDL below.

### Path A — Parquet Schema (DuckDB)

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

### Path B — Snowflake DDL (`sec_edgar.silver.*`)

Run once in Snowflake before the first pipeline execution.

```sql
-- silver.dim_security
CREATE TABLE IF NOT EXISTS sec_edgar.silver.dim_security (
  security_id            VARCHAR(16)   NOT NULL PRIMARY KEY,
  cik                    NUMBER(10)    NOT NULL,
  ticker                 VARCHAR(20)   NOT NULL,
  ticker_class           VARCHAR(5),
  company_name           VARCHAR(500),
  exchange               VARCHAR(50),
  sic                    VARCHAR(10),
  sic_description        VARCHAR(200),
  entity_type            VARCHAR(100),
  state_of_incorporation VARCHAR(5),
  fiscal_year_end        VARCHAR(4),
  category               VARCHAR(100),
  active_flag            BOOLEAN       NOT NULL DEFAULT TRUE,
  inactive_reason        VARCHAR(100),
  first_seen_date        DATE,
  last_seen_date         DATE,
  created_at             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- silver.filings_index
CREATE TABLE IF NOT EXISTS sec_edgar.silver.filings_index (
  security_id      VARCHAR(16)   NOT NULL,
  cik              NUMBER(10)    NOT NULL,
  accession_number VARCHAR(25)   NOT NULL,
  form_type        VARCHAR(20)   NOT NULL,
  filed_date       DATE          NOT NULL,
  period_of_report DATE,
  filing_url       VARCHAR(500),
  primary_document VARCHAR(200),
  is_xbrl          BOOLEAN,
  items            VARCHAR(100),
  ingested_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (security_id, accession_number)
);

-- silver.financial_facts
CREATE TABLE IF NOT EXISTS sec_edgar.silver.financial_facts (
  security_id          VARCHAR(16)   NOT NULL,
  cik                  NUMBER(10)    NOT NULL,
  period_end           DATE          NOT NULL,
  period_start         DATE,
  form_type            VARCHAR(20)   NOT NULL,
  filed_date           DATE          NOT NULL,
  fiscal_year          NUMBER(4),
  fiscal_period        VARCHAR(5),
  taxonomy             VARCHAR(20)   NOT NULL,
  revenues             NUMBER(22,2),
  net_income           NUMBER(22,2),
  operating_income     NUMBER(22,2),
  total_assets         NUMBER(22,2),
  total_liabilities    NUMBER(22,2),
  stockholders_equity  NUMBER(22,2),
  long_term_debt       NUMBER(22,2),
  cash_and_equivalents NUMBER(22,2),
  shares_outstanding   NUMBER(20),
  operating_cash_flow  NUMBER(22,2),
  eps_basic            NUMBER(14,4),
  eps_diluted          NUMBER(14,4),
  ingested_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (security_id, period_end, form_type)
);

-- silver.corporate_actions
CREATE TABLE IF NOT EXISTS sec_edgar.silver.corporate_actions (
  security_id              VARCHAR(16)   NOT NULL,
  cik                      NUMBER(10)    NOT NULL,
  event_type               VARCHAR(50)   NOT NULL,
  event_date               DATE          NOT NULL,
  effective_date           DATE,
  accession_number         VARCHAR(25),
  form_type                VARCHAR(20),
  filed_date               DATE,
  counterparty_cik         NUMBER(10),
  counterparty_name        VARCHAR(500),
  split_ratio_numerator    NUMBER(5),
  split_ratio_denominator  NUMBER(5),
  description              VARCHAR(1000),
  ingested_at              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
```

**`security_id` in Snowflake SQL:**
```sql
LEFT(SHA2(LPAD(CAST(cik AS VARCHAR), 10, '0') || '|' || UPPER(COALESCE(ticker_class, 'PRIMARY')), 256), 16)
```

---

## 10. Gold Layer

### Path A — Parquet Schema (DuckDB)

Gold Parquet files are rebuilt from Silver on every run. No incremental logic.

#### 10.1 `gold/financial_statements_annual/refreshed_date=*/data.parquet`

Columns: `security_id`, `cik`, `ticker`, `company_name`, `exchange`, `fiscal_year`, `period_end`, `filed_date`, `taxonomy`, `revenues`, `net_income`, `operating_income`, `total_assets`, `total_liabilities`, `stockholders_equity`, `long_term_debt`, `cash_and_equivalents`, `operating_cash_flow`, `eps_basic`, `eps_diluted`, plus derived ratios:
- `net_margin` = `net_income / revenues`
- `return_on_equity` = `net_income / stockholders_equity`
- `debt_to_equity` = `long_term_debt / stockholders_equity`

#### 10.2 `gold/financial_statements_quarterly/refreshed_date=*/data.parquet`

Same as annual plus `fiscal_period` (`'Q1'`–`'Q4'`). Most recent 8 quarters.

#### 10.3 `gold/company_profile/refreshed_date=*/data.parquet`

One row per security. Columns: `security_id`, `cik`, `ticker`, `company_name`, `exchange`, `sic`, `sic_description`, `state_of_incorporation`, `fiscal_year_end`, `active_flag`, `inactive_reason`, `latest_10k_date`, `latest_10q_date`, `latest_revenues`, `latest_total_assets`, `latest_shares_outstanding`, `latest_eps_diluted`.

#### 10.4 `gold/filing_catalog/refreshed_date=*/data.parquet`

All indexed filings. Columns: `security_id`, `cik`, `ticker`, `company_name`, `accession_number`, `form_type`, `filed_date`, `period_of_report`, `filing_url`, `primary_document`.

#### 10.5 `gold/corporate_events/refreshed_date=*/data.parquet`

Corporate events enriched with ticker and company name. Columns: `security_id`, `cik`, `ticker`, `company_name`, `event_type`, `event_date`, `effective_date`, `form_type`, `filed_date`, `description`.

---

### Path B — Snowflake DDL

Run `scripts/setup/snowflake/03_create_gold_tables.sql`. Gold tables are fully rebuilt by dbt `table` models on each run.

#### 10.6 `gold.financial_statements_annual`

```sql
CREATE TABLE IF NOT EXISTS sec_edgar.gold.financial_statements_annual (
  security_id          VARCHAR(16),
  cik                  NUMBER(10),
  ticker               VARCHAR(20),
  company_name         VARCHAR(500),
  exchange             VARCHAR(50),
  fiscal_year          NUMBER(4),
  period_end           DATE,
  filed_date           DATE,
  taxonomy             VARCHAR(20),
  revenues             NUMBER(22,2),
  net_income           NUMBER(22,2),
  operating_income     NUMBER(22,2),
  total_assets         NUMBER(22,2),
  total_liabilities    NUMBER(22,2),
  stockholders_equity  NUMBER(22,2),
  long_term_debt       NUMBER(22,2),
  cash_and_equivalents NUMBER(22,2),
  operating_cash_flow  NUMBER(22,2),
  eps_basic            NUMBER(14,4),
  eps_diluted          NUMBER(14,4),
  -- Derived ratios
  net_margin           NUMBER(10,6),
  return_on_equity     NUMBER(10,6),
  debt_to_equity       NUMBER(10,6),
  refreshed_at         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ticker, fiscal_year);
```

#### 10.7 `gold.financial_statements_quarterly`

```sql
CREATE TABLE IF NOT EXISTS sec_edgar.gold.financial_statements_quarterly (
  security_id          VARCHAR(16),
  cik                  NUMBER(10),
  ticker               VARCHAR(20),
  company_name         VARCHAR(500),
  exchange             VARCHAR(50),
  fiscal_year          NUMBER(4),
  fiscal_period        VARCHAR(5),
  period_end           DATE,
  filed_date           DATE,
  taxonomy             VARCHAR(20),
  revenues             NUMBER(22,2),
  net_income           NUMBER(22,2),
  operating_income     NUMBER(22,2),
  total_assets         NUMBER(22,2),
  total_liabilities    NUMBER(22,2),
  stockholders_equity  NUMBER(22,2),
  long_term_debt       NUMBER(22,2),
  cash_and_equivalents NUMBER(22,2),
  operating_cash_flow  NUMBER(22,2),
  eps_basic            NUMBER(14,4),
  eps_diluted          NUMBER(14,4),
  refreshed_at         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ticker, period_end);
```

#### 10.8 `gold.company_profile`

```sql
CREATE TABLE IF NOT EXISTS sec_edgar.gold.company_profile (
  security_id               VARCHAR(16),
  cik                       NUMBER(10),
  ticker                    VARCHAR(20),
  company_name              VARCHAR(500),
  exchange                  VARCHAR(50),
  sic                       VARCHAR(10),
  sic_description           VARCHAR(200),
  state_of_incorporation    VARCHAR(5),
  fiscal_year_end           VARCHAR(4),
  active_flag               BOOLEAN,
  inactive_reason           VARCHAR(50),
  latest_10k_date           DATE,
  latest_10q_date           DATE,
  latest_revenues           NUMBER(22,2),
  latest_total_assets       NUMBER(22,2),
  latest_shares_outstanding NUMBER(20),
  latest_eps_diluted        NUMBER(14,4),
  refreshed_at              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ticker);
```

#### 10.9 `gold.filing_catalog`

```sql
CREATE TABLE IF NOT EXISTS sec_edgar.gold.filing_catalog (
  security_id      VARCHAR(16),
  cik              NUMBER(10),
  ticker           VARCHAR(20),
  company_name     VARCHAR(500),
  accession_number VARCHAR(25),
  form_type        VARCHAR(20),
  filed_date       DATE,
  period_of_report DATE,
  filing_url       VARCHAR(500),
  primary_document VARCHAR(200),
  refreshed_at     TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ticker, filed_date);
```

#### 10.10 `gold.corporate_events`

```sql
CREATE TABLE IF NOT EXISTS sec_edgar.gold.corporate_events (
  security_id    VARCHAR(16),
  cik            NUMBER(10),
  ticker         VARCHAR(20),
  company_name   VARCHAR(500),
  event_type     VARCHAR(50),
  event_date     DATE,
  effective_date DATE,
  form_type      VARCHAR(20),
  filed_date     DATE,
  description    VARCHAR(1000),
  refreshed_at   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ticker, event_date);
```

---

---

## 11. SEC EDGAR API Reference

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

## 12. Pipeline Scripts

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
        return s3fs.S3FileSystem()         # boto3 credential chain — see Section 6.1
    if CLOUD_PROVIDER == "azure":
        import adlfs
        from azure.identity import DefaultAzureCredential
        return adlfs.AzureBlobFileSystem(
            account_name=AZURE_ACCOUNT,
            credential=DefaultAzureCredential()   # Managed Identity via AZURE_CLIENT_ID — see Section 6.2
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
    # Credentials: ECS task IAM role (production) or ~/.aws (local) — see Section 6.1
elif CLOUD_PROVIDER == "azure":
    conn.execute("LOAD azure;")
    # Credentials: AZURE_CLIENT_ID env var → DefaultAzureCredential → Managed Identity — see Section 6.2

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

### 11.7 Path B — `scripts/snowflake/load_bronze_to_snowflake.py`

Loads today's bronze Parquet batches from the External Stage into Snowflake bronze tables. Runs after all three ingest scripts. Uses `COPY INTO` (idempotent — skips already-loaded files via Snowflake's load history).

```python
# scripts/snowflake/load_bronze_to_snowflake.py
import snowflake.connector, os
from config.settings import INGEST_DATE

SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "sec_edgar")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "sec_edgar_wh")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "sec_edgar_loader")
SNOWFLAKE_KEY_PATH  = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]  # injected from Secrets Manager / Key Vault

from cryptography.hazmat.primitives.serialization import load_pem_private_key
with open(SNOWFLAKE_KEY_PATH, "rb") as f:
    private_key = load_pem_private_key(f.read(), password=None)

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    private_key=private_key,
    database=SNOWFLAKE_DATABASE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    role=SNOWFLAKE_ROLE,
)
cur = conn.cursor()

# COPY INTO is idempotent — Snowflake tracks loaded files in load history
for dataset, table in [
    (f"company_tickers_exchange/ingestion_date={INGEST_DATE}/", "bronze.company_tickers_exchange_raw"),
    (f"submissions/ingestion_date={INGEST_DATE}/",              "bronze.submissions_raw"),
    (f"companyfacts/ingestion_date={INGEST_DATE}/",             "bronze.companyfacts_raw"),
]:
    cur.execute(f"""
        COPY INTO {SNOWFLAKE_DATABASE}.{table}
        FROM @{SNOWFLAKE_DATABASE}.public.sec_edgar_stage/{dataset}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        PURGE = FALSE
    """)
    rows = cur.fetchone()[0]
    print(f"Loaded {rows} rows into {table}")

conn.close()
```

**Requirements:** Add `snowflake-connector-python>=3.5` and `cryptography>=41.0` to `requirements.txt` for Path B.

### 11.8 Path B — dbt Models (Silver + Gold)

dbt handles all Silver and Gold transforms in Snowflake. Models live under `dbt/models/`.

**`dbt/dbt_project.yml`:**
```yaml
name: sec_edgar
version: '1.0.0'
profile: sec_edgar

models:
  sec_edgar:
    silver:
      +schema: silver
      +materialized: incremental
    gold:
      +schema: gold
      +materialized: table
```

**`dbt/models/silver/dim_security.sql`** (incremental — upsert on `security_id`):
```sql
{{
  config(
    materialized = 'incremental',
    unique_key    = 'security_id',
    merge_update_columns = ['company_name', 'exchange', 'last_seen_date', 'updated_at']
  )
}}

SELECT
  LEFT(SHA2(LPAD(CAST(cik AS VARCHAR), 10, '0') || '|' || 'PRIMARY', 256), 16) AS security_id,
  cik,
  ticker,
  NULL::VARCHAR(5)    AS ticker_class,
  company_name,
  exchange,
  TRUE                AS active_flag,
  ingestion_date      AS first_seen_date,
  ingestion_date      AS last_seen_date,
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM {{ source('bronze', 'company_tickers_exchange_raw') }}
WHERE exchange IN ('NYSE', 'Nasdaq')
{% if is_incremental() %}
  AND ingestion_date = '{{ var("ingest_date") }}'
{% endif %}
```

**`dbt/models/silver/financial_facts.sql`** (incremental — upsert on `security_id + period_end + form_type`):
```sql
{{
  config(
    materialized = 'incremental',
    unique_key    = ['security_id', 'period_end', 'form_type'],
    merge_update_columns = ['revenues', 'net_income', 'operating_income', 'total_assets',
                             'total_liabilities', 'stockholders_equity', 'filed_date', 'ingested_at']
  )
}}

-- Parse XBRL facts from companyfacts_raw.facts_json
-- Uses Snowflake PARSE_JSON + lateral flatten to unnest us-gaap concept arrays
WITH raw AS (
  SELECT cik, ingestion_date,
         PARSE_JSON(facts_json) AS facts
  FROM {{ source('bronze', 'companyfacts_raw') }}
  {% if is_incremental() %}
  WHERE ingestion_date = '{{ var("ingest_date") }}'
  {% endif %}
),
revenues AS (
  SELECT cik,
         f.value:start::DATE   AS period_start,
         f.value:end::DATE     AS period_end,
         f.value:filed::DATE   AS filed_date,
         f.value:form::VARCHAR AS form_type,
         f.value:fp::VARCHAR   AS fiscal_period,
         f.value:val::NUMBER(22,2) AS revenues
  FROM raw,
  LATERAL FLATTEN(input =>
    COALESCE(
      facts['us-gaap']['Revenues']['units']['USD'],
      facts['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    )
  ) f
  WHERE DATEDIFF('day', f.value:start::DATE, f.value:end::DATE) BETWEEN 355 AND 375
    AND f.value:form IN ('10-K', '10-K/A')
)
-- Similar CTEs for net_income, total_assets, etc.
-- Final SELECT joins all CTEs on (cik, period_end, form_type)
-- keeping latest filed_date per (cik, period_end, form_type) for amendment handling
SELECT
  ds.security_id,
  r.cik,
  r.period_end,
  r.filed_date,
  r.form_type,
  'us-gaap'        AS taxonomy,
  r.revenues,
  -- other financial columns follow the same pattern
  CURRENT_TIMESTAMP() AS ingested_at
FROM revenues r
JOIN {{ ref('dim_security') }} ds ON r.cik = ds.cik
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.cik, r.period_end, r.form_type
                           ORDER BY r.filed_date DESC) = 1
```

**`dbt/models/gold/financial_statements_annual.sql`** (`table` — fully rebuilt each run):
```sql
{{ config(materialized='table') }}

SELECT
  s.security_id,
  s.cik,
  s.ticker,
  s.company_name,
  s.exchange,
  f.fiscal_year,
  f.period_end,
  f.filed_date,
  f.taxonomy,
  f.revenues,
  f.net_income,
  f.operating_income,
  f.total_assets,
  f.total_liabilities,
  f.stockholders_equity,
  f.long_term_debt,
  f.cash_and_equivalents,
  f.operating_cash_flow,
  f.eps_basic,
  f.eps_diluted,
  DIV0(f.net_income, f.revenues)            AS net_margin,
  DIV0(f.net_income, f.stockholders_equity) AS return_on_equity,
  DIV0(f.long_term_debt, f.stockholders_equity) AS debt_to_equity,
  CURRENT_TIMESTAMP()                       AS refreshed_at
FROM {{ ref('financial_facts') }} f
JOIN {{ ref('dim_security') }} s ON f.security_id = s.security_id
WHERE f.form_type IN ('10-K', '10-K/A')
```

**Running dbt:**
```bash
# Full run (initial load or full refresh)
dbt run --vars '{"ingest_date": "2024-01-25"}'

# Incremental update for today
dbt run --vars '{"ingest_date": "2024-01-25"}' --select silver.*
dbt run --select gold.*

# Run tests
dbt test
```

---

## 13. Orchestration

All three environments (local, AWS, Azure) run the **same Docker container** with different `CMD` overrides per task. Credentials are injected via environment variables; no secrets are baked into the image.

### Path A — Pipeline execution order (DuckDB / Parquet)

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

### Path B — Pipeline execution order (Snowflake + dbt)

```
[ingest_tickers]          ← Stage 1: same as Path A
         │
         ▼
[ingest_submissions]      ← Stage 2a: same as Path A (sequential)
         │
         ▼
[ingest_companyfacts]     ← Stage 2b: same as Path A (sequential)
         │
         ▼
[bronze_gate]             ← asserts Parquet file count > 0 for all 3 bronze datasets
         │
         ▼
[snowflake_copy_into]     ← COPY INTO all bronze Parquet batches → Snowflake bronze tables
         │                   (scripts/snowflake/load_bronze_to_snowflake.py — Section 11.7)
         ▼
[dbt_run_silver]          ← dbt run --select silver.* --vars '{"ingest_date": "..."}'
         │                   Incremental MERGE into dim_security, filings_index, financial_facts
         ▼
[dbt_test_silver]         ← dbt test --select silver.* (not_null, unique key assertions)
         │
         ▼
[dbt_run_gold]            ← dbt run --select gold.* (full table rebuild)
         │
         ▼
[dbt_test_gold]           ← dbt test --select gold.*
```

**Rule:** Same gate principle applies — `bronze_gate` must pass before `snowflake_copy_into` starts.

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

**Path A (`pipeline.py`):**
```python
# pipeline.py — Path A sequential local runner (DuckDB / Parquet)
import subprocess
from config.settings import INGEST_DATE

def run(args): subprocess.run(["python"] + args, check=True)

# Stage 1
run(["scripts/ingest/01_ingest_tickers_exchange.py", "--date", INGEST_DATE])
# Stage 2 — sequential (NOT parallel — respect SEC rate limit)
run(["scripts/ingest/02_ingest_submissions.py",  "--date", INGEST_DATE])
run(["scripts/ingest/03_ingest_companyfacts.py", "--date", INGEST_DATE])
# Bronze gate
run(["scripts/ingest/bronze_gate.py"])
# Silver — DuckDB transforms
run(["scripts/bronze_to_silver/01_silver_dim_security.py"])
run(["scripts/bronze_to_silver/02_silver_filings_and_facts.py"])
# Gold — DuckDB transforms
run(["scripts/silver_to_gold/01_build_gold.py"])
```

**Path B (`pipeline_snowflake.py`):**
```python
# pipeline_snowflake.py — Path B sequential local runner (Snowflake + dbt)
import subprocess
from config.settings import INGEST_DATE

def run(args): subprocess.run(["python"] + args, check=True)
def dbt(args): subprocess.run(["dbt"] + args, cwd="dbt", check=True)

# Stage 1-3: ingest is identical to Path A
run(["scripts/ingest/01_ingest_tickers_exchange.py", "--date", INGEST_DATE])
run(["scripts/ingest/02_ingest_submissions.py",  "--date", INGEST_DATE])
run(["scripts/ingest/03_ingest_companyfacts.py", "--date", INGEST_DATE])
run(["scripts/ingest/bronze_gate.py"])
# Stage 4: load bronze Parquet → Snowflake
run(["scripts/snowflake/load_bronze_to_snowflake.py"])
# Stage 5: dbt silver + gold
dbt(["run", "--select", "silver.*", "--vars", f'{{"ingest_date": "{INGEST_DATE}"}}'])
dbt(["test", "--select", "silver.*"])
dbt(["run", "--select", "gold.*"])
dbt(["test", "--select", "gold.*"])
```

---

## 14. Project File Layout

**Path A (DuckDB):**
```
sec_edgar_platform/
├── Dockerfile                              ← single image for ADF Batch, ECS Fargate, and local
├── requirements.txt                        ← pyarrow, s3fs, adlfs, azure-identity, duckdb, requests
├── pipeline.py                             ← Path A: local/dev sequential runner
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
│   └── iam/
│       ├── ecs_task_trust.json             ← AWS: trust policy for ECS task + execution roles
│       ├── ecs_task_role_policy.json       ← AWS: S3 read/write scoped to bucket/prefix
│       └── sfn_role_policy.json            ← AWS: Step Functions role (ecs:RunTask + iam:PassRole)
└── tests/
    └── test_spot_check.py                  ← DuckDB queries over S3/Azure Parquet for verification
```

**Path B additions (Snowflake + dbt):**
```
sec_edgar_platform/
├── pipeline_snowflake.py                   ← Path B: local/dev runner (replaces pipeline.py)
├── requirements_snowflake.txt              ← adds: snowflake-connector-python, cryptography, dbt-snowflake
├── scripts/
│   ├── snowflake/
│   │   └── load_bronze_to_snowflake.py    ← COPY INTO bronze Parquet → Snowflake bronze tables
│   └── setup/
│       └── snowflake/
│           ├── 01_create_schemas.sql       ← CREATE DATABASE / SCHEMA / WAREHOUSE / ROLE
│           ├── 02_create_silver_tables.sql ← Silver DDL (Section 9 Path B)
│           ├── 03_create_gold_tables.sql   ← Gold DDL (Section 10 Path B)
│           └── 04_create_stage.sql         ← External Stage + Storage Integration (Section 6.4)
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml                        ← Snowflake connection (key-pair auth — Section 6.4)
    ├── sources.yml                         ← declares bronze tables as dbt sources
    ├── models/
    │   ├── silver/
    │   │   ├── dim_security.sql            ← incremental MERGE (Section 11.8)
    │   │   ├── filings_index.sql           ← incremental MERGE
    │   │   ├── financial_facts.sql         ← incremental MERGE (Section 11.8)
    │   │   └── corporate_actions.sql       ← incremental MERGE
    │   └── gold/
    │       ├── financial_statements_annual.sql     ← table (Section 11.8)
    │       ├── financial_statements_quarterly.sql  ← table
    │       ├── company_profile.sql                 ← table
    │       ├── filing_catalog.sql                  ← table
    │       └── corporate_events.sql                ← table
    └── tests/
        ├── silver_not_null.yml             ← dbt not_null/unique tests on silver primary keys
        └── gold_not_null.yml               ← dbt tests on gold
```

**No `scripts/setup/` SQL files for Path A** — there is no database to initialize. Parquet directories are created automatically by the first write.

---

## 15. Verification Steps

### Path A — DuckDB / Parquet

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

### Step 8 — Idempotency check (Path A)
Re-run the pipeline for the same date. Silver/Gold Parquet files are overwritten with identical content (same row counts). Bronze skips files that already exist.

---

### Path B — Snowflake

### Step B-1 — Snowflake connectivity
```bash
snowsql -a {SNOWFLAKE_ACCOUNT} -u {SNOWFLAKE_USER} --private-key-path ~/.snowflake/rsa_key.p8
# Expected: SnowSQL prompt; run: SELECT CURRENT_USER(), CURRENT_ROLE();
```

### Step B-2 — Bronze tables loaded
```sql
SELECT ingestion_date, COUNT(*) AS rows
FROM sec_edgar.bronze.company_tickers_exchange_raw
GROUP BY ingestion_date ORDER BY ingestion_date DESC LIMIT 5;
-- Expected: today's date with 10,000+ rows

SELECT ingestion_date, COUNT(*) AS rows
FROM sec_edgar.bronze.submissions_raw
GROUP BY ingestion_date ORDER BY ingestion_date DESC LIMIT 5;
-- Expected: today's date with 6,000+ rows (NYSE + Nasdaq)
```

### Step B-3 — External stage accessible
```sql
LIST @sec_edgar.public.sec_edgar_stage/bronze/company_tickers_exchange/;
-- Expected: today's batch Parquet files listed; no error
```

### Step B-4 — Silver security master (after dbt run)
```sql
SELECT COUNT(*) AS total_securities,
       SUM(CASE WHEN active_flag THEN 1 ELSE 0 END) AS active
FROM sec_edgar.silver.dim_security;
-- Expected: ~6,000–8,000 active (NYSE + Nasdaq)
```

### Step B-5 — Spot check known tickers
```sql
SELECT s.ticker, s.company_name, f.period_end, f.revenues, f.net_income
FROM sec_edgar.silver.dim_security s
JOIN sec_edgar.silver.financial_facts f ON s.security_id = f.security_id
WHERE s.ticker IN ('AAPL', 'MSFT', 'TSLA', 'JPM')
  AND f.form_type = '10-K'
ORDER BY s.ticker, f.period_end DESC;
-- Expected: 4+ rows per ticker with positive revenues
```

### Step B-6 — Gold tables ready
```sql
SELECT ticker, fiscal_year, revenues, net_margin, return_on_equity
FROM sec_edgar.gold.financial_statements_annual
WHERE ticker IN ('AAPL', 'MSFT')
ORDER BY ticker, fiscal_year DESC
LIMIT 10;
```

### Step B-7 — Full pipeline run (Path B)
```bash
# Local
python pipeline_snowflake.py

# dbt docs (optional — generates HTML lineage graph)
cd dbt && dbt docs generate && dbt docs serve
```

### Step B-8 — Idempotency check (Path B)
Re-run `dbt run` for the same `ingest_date`. Silver rows are upserted (no duplicates — unique key constraint). Gold is fully rebuilt. Row counts should be identical.

---

## 16. Edge Cases and Error Handling

| Scenario | Handling |
|---|---|
| Company uses IFRS (not us-gaap) | Try us-gaap facts first, then ifrs-full; set `taxonomy = 'ifrs-full'` |
| `companyfacts` returns 404 | Log CIK to checkpoint; write NULL financial facts; do not retry on same day |
| Multi-class shares (BRK.A/BRK.B) | Both share same CIK; `ticker_class` differentiates; both get separate `security_id` |
| HTTP 429 rate limit | Sleep 60s, retry up to 3× with exponential backoff |
| `submissions.filings.files` pagination | Fetch continuation pages, cap at 5 pages per CIK |
| SEC rate limit across tasks | Ingest tasks run **sequentially** — each uses ≤8 req/s; never exceed 10 req/s total |
| DuckDB extensions in VPC | Extensions are pre-installed in the Dockerfile — no runtime internet download needed |
| DuckDB S3 auth (ECS) | ECS task IAM role via instance metadata — no env vars needed (Section 6.1.1) |
| DuckDB Azure auth (Batch) | `AZURE_CLIENT_ID` on Batch pool → `DefaultAzureCredential` → Managed Identity (Section 6.2.4) |
| SEC site maintenance (weekends) | Pipeline is idempotent — next run rewrites today's Silver/Gold Parquet |
| Large companyfacts payload (>10 MB) | Store as-is in Parquet STRING column; DuckDB parses JSON in Silver transform |
| ADF Batch pool node unavailable | ADF retries automatically; Batch scales pool nodes up/down |
| ECS Fargate task fails | Step Functions retry policy (`MaxAttempts: 2`) automatically retries failed states |

---

## 17. How to Start Coding (Post-Review Checklist)

> ### ❓ Decision Gate — Choose your Silver/Gold destination before proceeding
>
> | Question | Answer |
> |---|---|
> | Do you already have a Snowflake account (or budget for one)? | If **yes** → **Path B (Snowflake)**. If **no** → **Path A (DuckDB)**. |
> | Do you need dbt tests, lineage docs, or BI tool SQL access? | If **yes** → **Path B**. |
> | Is cost sensitivity or minimal infrastructure your priority? | If **yes** → **Path A**. |
> | Are you prototyping or running on a laptop? | **Path A** — no external service needed. |
>
> **Set your path now.** Write it in `config/settings.py` or your `.env`:
> ```bash
> SILVER_GOLD_PATH=duckdb     # Path A — DuckDB writes Parquet to S3/Azure
> # or
> SILVER_GOLD_PATH=snowflake  # Path B — COPY INTO + dbt-snowflake
> ```
> Then follow only the phases for your chosen path below.

---

### Phase 0 — Environment Setup (Do Once — Both Paths)

- [ ] **Choose cloud provider**: set `CLOUD_PROVIDER=aws` or `CLOUD_PROVIDER=azure`.
- [ ] **Provision storage** (Section 5): create S3 bucket or ADLS Gen2 storage account + container with Hierarchical Namespace enabled.
- [ ] **Configure credentials and IAM/RBAC** — follow **Section 6** completely before running any script:
  - AWS: create ECS Task Role + Execution Role + Step Functions Role (Section 6.1); locally run `aws configure`
  - Azure: create Managed Identity, assign RBAC roles, configure Batch pool identity (Section 6.2); locally run `az login`
- [ ] **Create Python virtual environment**:
  ```bash
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  ```

### Phase 1 — Create the Project Scaffold (Both Paths)

```bash
mkdir -p config scripts/{ingest,bronze_to_silver,silver_to_gold} workflows tests
touch config/__init__.py config/settings.py
```

Copy the settings template from **Section 4** into `config/settings.py` and fill in your values.

### Phase 2 — Write and Test the Ingestion Scripts (Both Paths)

These scripts are **identical for both paths** — bronze always lands as Parquet in S3/Azure.

1. **`scripts/ingest/_rate_limiter.py`** — Section 12 (11.0)
2. **`scripts/ingest/_http.py`** — Section 12 (11.0)
3. **`scripts/ingest/_batch_writer.py`** — Section 12 (11.0) — uses fsspec/s3fs/adlfs
4. **`scripts/ingest/01_ingest_tickers_exchange.py`** — Section 12 (11.1)
   - Test: `INGEST_DATE=2024-01-25 python scripts/ingest/01_ingest_tickers_exchange.py`
   - Verify: Parquet appears in `{STORAGE_ROOT}/bronze/company_tickers_exchange/ingestion_date=2024-01-25/`
   - Check row count ≈ 10,000–12,000
5. **`scripts/ingest/02_ingest_submissions.py`** — Section 12 (11.2) — dev test: `--limit 5`
6. **`scripts/ingest/03_ingest_companyfacts.py`** — Section 12 (11.3) — dev test: `--limit 5`
7. **`scripts/ingest/bronze_gate.py`** — asserts all 3 datasets have Parquet files before proceeding

---

### Path A — DuckDB / Parquet (continue here if SILVER_GOLD_PATH=duckdb)

### Phase A-3 — Write Silver Transforms (DuckDB)

1. **`scripts/bronze_to_silver/01_silver_dim_security.py`** — Section 12 (11.4)
   - The `security_id` SHA-256 hash is the most critical piece — test with known CIKs first (Section 9 Path A)
   - Verify: `python -c "import duckdb; c=duckdb.connect(); print(c.execute(\"SELECT COUNT(*) FROM read_parquet('.../silver/dim_security/*/*.parquet')\").fetchone())"`

2. **`scripts/bronze_to_silver/02_silver_filings_and_facts.py`** — Section 12 (11.5)
   - Start with `filings_index` (simpler JSON parsing), then add `financial_facts`
   - Key complexity: unnesting parallel arrays from `filings_recent_json` and XBRL concept extraction

### Phase A-4 — Write Gold Transforms (DuckDB)

**`scripts/silver_to_gold/01_build_gold.py`** — Section 12 (11.6)

Start with `financial_statements_annual` (highest value). Each gold table is a single DuckDB `COPY ... TO` with a `SELECT ... JOIN` of silver Parquet. Verify with Section 15 Steps 4–6.

### Phase A-5 — Wire Up Cloud Orchestration

**Azure:** Create `workflows/adf_pipeline.json` (Section 13, Option A — ADF). Deploy via ADF portal or ARM.

**AWS:** Create `workflows/step_functions_definition.json` + `workflows/ecs_task_definition.json` (Section 13, Option B — Step Functions). Deploy via `aws cloudformation` or Terraform.

### Phase A-6 — Full Verification (Path A)

Run Steps 1–8 from **Section 15 (Path A)** in order.

---

### Path A — Implementation Priority

| Priority | Deliverable | Sections |
|---|---|---|
| 1 (highest) | Bronze tickers + silver `dim_security` | 6, 7.1, 9 Path A (9.1), 12 (11.1–11.4) |
| 2 | Bronze submissions + silver `filings_index` | 7.2, 9 Path A (9.2), 12 (11.2) |
| 3 | Bronze companyfacts + silver `financial_facts` | 7.3, 9 Path A (9.3), 12 (11.3, 11.5) |
| 4 | Gold annual financials | 10 Path A (10.1), 12 (11.6) |
| 5 | All other gold tables | 10 Path A (10.2–10.5) |
| 6 | Cloud orchestration (ADF or Step Functions) | 13 |

---

### Path B — Snowflake + dbt (continue here if SILVER_GOLD_PATH=snowflake)

### Phase B-3 — Snowflake Setup (One-Time)

- [ ] **Configure Snowflake auth** — generate RSA key pair, add public key to service user (Section 6.4)
- [ ] **Create Storage Integration** (Section 6.4) — runs as `ACCOUNTADMIN`; provides Snowflake an IAM role (AWS) or Managed Identity (Azure) to read from the S3/Azure stage
- [ ] **Install additional Python deps**:
  ```bash
  pip install snowflake-connector-python>=3.5 cryptography>=41.0 dbt-snowflake>=1.7
  ```
- [ ] **Run Snowflake setup SQL** in order:
  ```bash
  snowsql -a {ACCOUNT} -u {USER} -f scripts/setup/snowflake/01_create_schemas.sql
  snowsql -a {ACCOUNT} -u {USER} -f scripts/setup/snowflake/02_create_silver_tables.sql
  snowsql -a {ACCOUNT} -u {USER} -f scripts/setup/snowflake/03_create_gold_tables.sql
  snowsql -a {ACCOUNT} -u {USER} -f scripts/setup/snowflake/04_create_stage.sql
  ```
- [ ] **Configure dbt profile** (`dbt/profiles.yml`) — see Section 6.4; set `private_key_path`
- [ ] **Verify dbt connection**: `cd dbt && dbt debug`

### Phase B-4 — Load Bronze into Snowflake

**`scripts/snowflake/load_bronze_to_snowflake.py`** — Section 12 (11.7)

After each ingest run: `python scripts/snowflake/load_bronze_to_snowflake.py`

Verify Step B-2 in **Section 15 (Path B)** — bronze tables have today's rows.

### Phase B-5 — Write and Run dbt Silver Models

1. Create **`dbt/models/silver/dim_security.sql`** — Section 12 (11.8)
   - Test first full run: `dbt run --select silver.dim_security --vars '{"ingest_date": "2024-01-25"}'`
   - Verify: `SELECT COUNT(*) FROM sec_edgar.silver.dim_security;` → 6,000+
2. Create **`dbt/models/silver/financial_facts.sql`** — Section 12 (11.8)
   - The XBRL LATERAL FLATTEN pattern is the most complex piece — test with CIK 320193 (Apple)
3. Create remaining silver models: `filings_index.sql`, `corporate_actions.sql`
4. Run `dbt test --select silver.*` — fix any not_null / unique failures before proceeding

### Phase B-6 — Write and Run dbt Gold Models

Create all 5 gold models under `dbt/models/gold/` — Section 12 (11.8). Each is a `{{ config(materialized='table') }}` `SELECT ... JOIN` of silver refs.

```bash
dbt run --select gold.*
dbt test --select gold.*
```

Verify with Steps B-5 and B-6 from **Section 15 (Path B)**.

### Phase B-7 — Wire Up Cloud Orchestration (Path B)

Add two extra tasks to the ADF or Step Functions pipeline after `bronze_gate`:

- **`snowflake_copy_into`** — runs `scripts/snowflake/load_bronze_to_snowflake.py`
- **`dbt_run_silver`** — runs `dbt run --select silver.* --vars ...`
- **`dbt_test_silver`** — runs `dbt test --select silver.*`
- **`dbt_run_gold`** — runs `dbt run --select gold.*`
- **`dbt_test_gold`** — runs `dbt test --select gold.*`

See Section 13 Path B pipeline order diagram for full dependency chain.

### Phase B-8 — Full Verification (Path B)

Run Steps B-1 through B-8 from **Section 15 (Path B)** in order.

---

### Path B — Implementation Priority

| Priority | Deliverable | Sections |
|---|---|---|
| 1 (highest) | Snowflake setup + bronze COPY INTO + dbt `dim_security` | 6.4, 9 Path B (9.1), 10 Path B (10.8), 12 (11.7–11.8) |
| 2 | dbt `filings_index` | 9 Path B (9.2), 12 (11.8) |
| 3 | dbt `financial_facts` | 9 Path B (9.3), 12 (11.8) |
| 4 | dbt gold `financial_statements_annual` | 10 Path B (10.6), 12 (11.8) |
| 5 | All other gold models | 10 Path B (10.7–10.10) |
| 6 | Cloud orchestration with Snowflake + dbt tasks | 13 |

**Start with Priority 1.** You'll have a queryable Snowflake `dim_security` table with `security_id` before writing a single financial fact row.
