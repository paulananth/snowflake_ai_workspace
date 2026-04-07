# Microsoft Entra ID Setup — SEC EDGAR Platform

Step-by-step guide to create managed identities, role-based access control (RBAC) assignments, and Microsoft Entra ID (formerly Azure Active Directory) configuration required to run the SEC EDGAR ingestion and transform pipeline on Azure (Azure Data Factory, Azure Batch, and Azure Data Lake Storage Gen2).

---

## Estimated Monthly Cost

| Resource | SKU / Config | Est. Cost/month |
|---|---|---|
| Azure Data Lake Storage Gen2 | Standard LRS, ~15 GB, Hot → Cool lifecycle | ~$0.40 |
| Azure Batch compute | Low-priority `Standard_D2s_v3`, ~90 min/day, scale-to-zero | ~$0.85 |
| Azure Container Registry | Basic tier | ~$5.00 |
| Azure Data Factory | ~8 activity runs/day | ~$0.25 |
| Azure Batch account | (no charge for account itself) | $0.00 |
| **Total** | | **~$6.50/month** |

**Key cost levers used in this guide:**
- Low-priority Batch nodes (80% cheaper than dedicated)
- Pool auto-scales to **0 nodes when idle** — no idle compute charge
- `Standard_D2s_v3` (2 vCPU) instead of D4s_v3 — tasks run sequentially, not in parallel
- Storage lifecycle policy moves Bronze Parquet older than 30 days to Cool tier (~50% cheaper storage)
- ACR Basic is the cheapest registry tier

---

## Prerequisites

- Azure subscription with Owner or User Access Administrator + Contributor rights
- Azure CLI installed and authenticated: `az login`
- Decisions made:
  - Resource group name (replace `{RG}` throughout)
  - Azure region (replace `{LOCATION}` — `eastus` or `westus2` are typically cheapest)
  - Storage account name (replace `{STORAGE_ACCOUNT}` — must be globally unique)
  - Container name (replace `{CONTAINER}` — e.g. `sec-edgar`)
  - Storage key prefix (replace `{PREFIX}` — e.g. `sec-edgar`)
  - Azure Batch account name (replace `{BATCH_ACCOUNT}`)
  - Azure Container Registry name (replace `{ACR_NAME}`)
  - Azure Data Factory name (replace `{ADF_NAME}`)

---

## Step 0 — Set Shell Variables

Run these once in your terminal. Every command below uses them.

```bash
SUBSCRIPTION=$(az account show --query id --output tsv)
RG=my-sec-edgar-rg
LOCATION=eastus          # eastus / westus2 are cheapest regions
STORAGE_ACCOUNT=mysecedgarstorage    # globally unique, lowercase, 3-24 chars, no hyphens
CONTAINER=sec-edgar
PREFIX=sec-edgar
BATCH_ACCOUNT=mysecedgarbatch
ACR_NAME=mysecedgaracr
ADF_NAME=mysecedgaradf
```

---

## Step 1 — Create the Resource Group

```bash
az group create \
  --name $RG \
  --location $LOCATION
```

---

## Step 2 — Create the Azure Data Lake Storage Gen2 Account

**Hierarchical namespace must be enabled** — this activates Azure Data Lake Storage Gen2 and is required for the `abfss://` scheme used in the pipeline.

```bash
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --access-tier Hot

# Create the container (filesystem in Azure Data Lake Storage Gen2 terminology)
az storage fs create \
  --name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login
```

**Verify HNS is enabled:**
```bash
az storage account show \
  --name $STORAGE_ACCOUNT --resource-group $RG \
  --query isHnsEnabled --output tsv
# Expected: true
```

### 2a. Storage Lifecycle Policy (cost optimisation)

Bronze Parquet files are write-once and rarely re-read after 30 days. Moving them to Cool tier cuts storage cost by ~50% for aged data. Silver and Gold are excluded — they are rewritten daily and must stay Hot.

```bash
az storage account management-policy create \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RG \
  --policy '{
    "rules": [
      {
        "name": "bronze-to-cool",
        "enabled": true,
        "type": "Lifecycle",
        "definition": {
          "filters": {
            "blobTypes": ["blockBlob"],
            "prefixMatch": ["'"$PREFIX"'/bronze/"]
          },
          "actions": {
            "baseBlob": {
              "tierToCool": { "daysAfterModificationGreaterThan": 30 },
              "tierToArchive": { "daysAfterModificationGreaterThan": 365 }
            }
          }
        }
      }
    ]
  }'
```

---

## Step 3 — Create the Azure Container Registry (ACR)

`Basic` is the cheapest tier (~$5/month). It is sufficient for a single image pulled by a small Batch pool.

```bash
az acr create \
  --name $ACR_NAME \
  --resource-group $RG \
  --location $LOCATION \
  --sku Basic \
  --admin-enabled false

ACR_ID=$(az acr show --name $ACR_NAME --resource-group $RG --query id --output tsv)
echo "ACR ID: $ACR_ID"
echo "ACR login server: $(az acr show --name $ACR_NAME --query loginServer --output tsv)"
```

---

## Step 4 — Create the User-Assigned Managed Identity

This identity is assigned to the Azure Batch pool nodes. Your Python containers running on those nodes inherit it automatically — no secrets or service principal credentials needed.

```bash
az identity create \
  --name sec-edgar-ingest-identity \
  --resource-group $RG \
  --location $LOCATION

# Save identifiers — used in later steps and as environment variables
IDENTITY_ID=$(az identity show \
  --name sec-edgar-ingest-identity --resource-group $RG \
  --query id --output tsv)

CLIENT_ID=$(az identity show \
  --name sec-edgar-ingest-identity --resource-group $RG \
  --query clientId --output tsv)

PRINCIPAL_ID=$(az identity show \
  --name sec-edgar-ingest-identity --resource-group $RG \
  --query principalId --output tsv)

echo "Identity resource ID:  $IDENTITY_ID"
echo "Client ID (env var):   $CLIENT_ID"
echo "Principal ID (for RBAC): $PRINCIPAL_ID"
```

**The `CLIENT_ID` value** must be set as the `AZURE_CLIENT_ID` environment variable on the Batch pool (Step 9a). This tells `DefaultAzureCredential` which managed identity to use.

---

## Step 5 — Grant RBAC Roles to the Managed Identity

### 5a. Storage Blob Data Contributor (Azure Data Lake Storage Gen2)

Allows the container to read and write Parquet files in the storage account.

```bash
STORAGE_SCOPE="/subscriptions/${SUBSCRIPTION}/resourceGroups/${RG}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}"

az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --scope $STORAGE_SCOPE
```

### 5b. AcrPull (Azure Container Registry)

Allows Batch pool nodes to pull the Docker image without a registry password.

```bash
az role assignment create \
  --role "AcrPull" \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --scope $ACR_ID
```

**Why these specific roles:**

| Role | Scope | Reason |
|---|---|---|
| `Storage Blob Data Contributor` | Storage account | Read + write blobs; does **not** grant storage account management or key access |
| `AcrPull` | Container registry | Pull images only; cannot push or delete images |

Do not use `Contributor` or `Owner` on the storage account — those grant control-plane access (key rotation, account deletion) that the pipeline does not need.

---

## Step 6 — Create the Azure Batch Account

```bash
az batch account create \
  --name $BATCH_ACCOUNT \
  --resource-group $RG \
  --location $LOCATION \
  --storage-account $STORAGE_ACCOUNT

BATCH_SCOPE=$(az batch account show \
  --name $BATCH_ACCOUNT --resource-group $RG --query id --output tsv)
echo "Batch account resource ID: $BATCH_SCOPE"
```

---

## Step 7 — Create the Azure Data Factory Instance

ADF charges only per activity run (~$0.001 each) — there is no idle cost for the factory itself.

```bash
az datafactory create \
  --factory-name $ADF_NAME \
  --resource-group $RG \
  --location $LOCATION

# Get the factory system-assigned managed identity principal ID
ADF_PRINCIPAL=$(az datafactory show \
  --factory-name $ADF_NAME --resource-group $RG \
  --query identity.principalId --output tsv)

echo "Azure Data Factory principal ID: $ADF_PRINCIPAL"
```

---

## Step 8 — Grant the Data Factory Identity Access to Azure Batch and Storage

Azure Data Factory uses its own system-assigned managed identity to connect to Azure Batch (to submit jobs) and to the storage account (to pass scripts to the Batch pool).

### 8a. Contributor on the Azure Batch account

Allows Azure Data Factory to create and monitor Batch jobs.

```bash
az role assignment create \
  --role "Contributor" \
  --assignee-object-id $ADF_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --scope $BATCH_SCOPE
```

### 8b. Storage Blob Data Reader on the Storage Account

Allows Azure Data Factory to read script files from the storage container when setting up Custom Activity tasks.

```bash
az role assignment create \
  --role "Storage Blob Data Reader" \
  --assignee-object-id $ADF_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --scope $STORAGE_SCOPE
```

---

## Step 9 — Create and Configure the Azure Batch Pool (cost-optimised)

**Cost optimisations applied here:**
- `Standard_D2s_v3` (2 vCPU, 8 GB RAM) — pipeline tasks are sequential; a 4-vCPU node is unused capacity
- `--target-low-priority-nodes 1 --target-dedicated-nodes 0` — low-priority nodes cost ~80% less than dedicated
- Auto-scale formula scales the pool to **0 nodes** when no tasks are queued — eliminates idle compute cost entirely

```bash
az batch account login \
  --name $BATCH_ACCOUNT \
  --resource-group $RG

# Create the pool with low-priority nodes and auto-scale
az batch pool create \
  --id sec-edgar-pool \
  --account-name $BATCH_ACCOUNT \
  --vm-size Standard_D2s_v3 \
  --target-dedicated-nodes 0 \
  --target-low-priority-nodes 0 \
  --image canonical:0001-com-ubuntu-server-jammy:22_04-lts \
  --node-agent-sku-id "batch.node.ubuntu 22.04" \
  --identity $IDENTITY_ID \
  --enable-auto-scale \
  --auto-scale-evaluation-interval "PT5M" \
  --auto-scale-formula "
    startingNumberOfVMs = 0;
    maxNumberofVMs = 1;
    pendingTaskSamplePercent = \$PendingTasks.GetSamplePercent(180 * TimeInterval_Second);
    pendingTaskSamples = pendingTaskSamplePercent < 70
      ? startingNumberOfVMs
      : avg(\$PendingTasks.GetSample(180 * TimeInterval_Second));
    \$TargetLowPriorityNodes = min(maxNumberofVMs, pendingTaskSamples);
    \$NodeDeallocationOption = taskcompletion;
  "
```

**Auto-scale behaviour:**
- When ADF submits a task → pool scales up to 1 low-priority node within ~5 minutes
- When all tasks complete → pool scales back to 0 nodes; no further compute charges
- `taskcompletion` deallocation option waits for the running task to finish before removing a node

> **Note:** The full `containerConfiguration` block (specifying the ACR image and container registry) must be set in the pool's JSON definition via the Azure portal or ARM template. Set `containerConfiguration.type = DockerCompatible` and add the ACR login server under `containerRegistries` with `identityReference` pointing to `$IDENTITY_ID`.

### 9a. Set the AZURE_CLIENT_ID environment variable on the pool

When multiple managed identities are present on a Batch node, `DefaultAzureCredential` needs a hint. Add this to the pool's `environmentSettings`:

```json
{
  "environmentSettings": [
    {
      "name": "AZURE_CLIENT_ID",
      "value": "<CLIENT_ID from Step 4>"
    }
  ]
}
```

Set this via the Azure portal (Batch account → Pools → sec-edgar-pool → Environment settings) or include it in the ARM/Bicep template for the pool.

---

## Step 10 — Push Docker Image to the Azure Container Registry

```bash
az acr login --name $ACR_NAME

ACR_SERVER=$(az acr show --name $ACR_NAME --query loginServer --output tsv)

docker build -t ${ACR_SERVER}/sec-edgar-ingest:latest .
docker push ${ACR_SERVER}/sec-edgar-ingest:latest

echo "Image URI: ${ACR_SERVER}/sec-edgar-ingest:latest"
```

---

## Step 11 (Path B only) — Snowflake Azure Storage Integration

Skip this step if using Path A (DuckDB).

Snowflake reads Bronze-layer Parquet from Azure Data Lake Storage Gen2 via a storage integration. After `CREATE STORAGE INTEGRATION` and `DESC INTEGRATION sec_edgar_adls_int` in Snowflake, note `AZURE_CONSENT_URL` in the output — open it in a browser to complete the Microsoft Entra ID admin consent flow.

### 11a. Prepare the Storage Integration in Snowflake

```sql
-- Run as ACCOUNTADMIN in Snowflake
CREATE STORAGE INTEGRATION sec_edgar_adls_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://<STORAGE_ACCOUNT>.blob.core.windows.net/<CONTAINER>/<PREFIX>/bronze/'
  );

-- Replace angle-bracket placeholders with literal Azure resource names (not shell variables).

DESC INTEGRATION sec_edgar_adls_int;
-- Note the AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME values
```

### 11b. Grant Snowflake's Application Access to Azure Data Lake Storage Gen2

After opening `AZURE_CONSENT_URL` in a browser (consents to Snowflake's multi-tenant application in your tenant):

```bash
SNOWFLAKE_APP_NAME="<paste value from DESC INTEGRATION>"
SNOWFLAKE_PRINCIPAL=$(az ad sp list \
  --display-name $SNOWFLAKE_APP_NAME \
  --query "[0].id" --output tsv)

# Grant Storage Blob Data Reader — Snowflake only reads Bronze-layer Parquet
az role assignment create \
  --role "Storage Blob Data Reader" \
  --assignee-object-id $SNOWFLAKE_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --scope $STORAGE_SCOPE
```

---

## Step 12 — Local Dev (Azure)

For local development, `DefaultAzureCredential` uses your `az login` token automatically — no service principal or managed identity needed locally.

```bash
az login
az account set --subscription $SUBSCRIPTION

# Grant your own user blob access for local dev
MY_PRINCIPAL=$(az ad signed-in-user show --query id --output tsv)

az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id $MY_PRINCIPAL \
  --assignee-principal-type User \
  --scope $STORAGE_SCOPE

# Test storage access
az storage blob list \
  --container-name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --prefix "${PREFIX}/" \
  --auth-mode login
```

---

## Verification

### Confirm all role assignments exist

```bash
echo "=== Managed Identity (sec-edgar-ingest-identity) ==="
az role assignment list \
  --assignee $PRINCIPAL_ID \
  --query "[].{Role:roleDefinitionName, Scope:scope}" \
  --output table

echo "=== Azure Data Factory identity ==="
az role assignment list \
  --assignee $ADF_PRINCIPAL \
  --query "[].{Role:roleDefinitionName, Scope:scope}" \
  --output table
```

Expected output for the Batch pool managed identity:
```
Role                          Scope
----------------------------  -----------------------------------------------
Storage Blob Data Contributor  .../storageAccounts/{STORAGE_ACCOUNT}
AcrPull                        .../registries/{ACR_NAME}
```

Expected output for the Azure Data Factory managed identity:
```
Role                      Scope
------------------------  -----------------------------------------------
Contributor               .../batchAccounts/{BATCH_ACCOUNT}
Storage Blob Data Reader  .../storageAccounts/{STORAGE_ACCOUNT}
```

### Confirm HNS is enabled on storage account

```bash
az storage account show \
  --name $STORAGE_ACCOUNT --resource-group $RG \
  --query isHnsEnabled --output tsv
# Expected: true
```

### Confirm auto-scale formula is active on the pool

```bash
az batch pool show \
  --pool-id sec-edgar-pool \
  --account-name $BATCH_ACCOUNT \
  --query "{autoScaleEnabled:enableAutoScale, formula:autoScaleFormula}" \
  --output json
# Expected: enableAutoScale: true, formula present
```

### Confirm storage lifecycle policy was applied

```bash
az storage account management-policy show \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RG \
  --query "policy.rules[].{Name:name, Enabled:enabled}" \
  --output table
# Expected: bronze-to-cool / true
```

### Test blob write from local machine

```bash
echo "test" > /tmp/test.txt
az storage blob upload \
  --container-name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --name "${PREFIX}/test.txt" \
  --file /tmp/test.txt \
  --auth-mode login && echo "Upload OK"

az storage blob delete \
  --container-name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --name "${PREFIX}/test.txt" \
  --auth-mode login
```

---

## Summary — Resource IDs to Record

Copy these values into your Azure Data Factory linked service definitions, pipeline parameters, and `config/settings.py`:

| Resource | Value |
|---|---|
| Storage account name | `{STORAGE_ACCOUNT}` |
| Container name | `{CONTAINER}` |
| Azure Data Lake Storage Gen2 URL | `abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{PREFIX}` |
| Managed identity client ID (`AZURE_CLIENT_ID`) | Output of Step 4 |
| Managed identity resource ID | Output of Step 4 |
| Azure Container Registry login server | `{ACR_NAME}.azurecr.io` |
| Docker image URI | `{ACR_NAME}.azurecr.io/sec-edgar-ingest:latest` |
| Azure Data Factory name | `{ADF_NAME}` |
| Azure Batch account name | `{BATCH_ACCOUNT}` |
| Batch pool ID | `sec-edgar-pool` |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `AuthorizationPermissionMismatch` when writing blobs | Missing `Storage Blob Data Contributor` on the identity | Re-run Step 5a; wait up to 5 minutes for RBAC propagation |
| `abfss://` path not found | Hierarchical Namespace not enabled on the storage account | Cannot be enabled after creation — create a new account with `--enable-hierarchical-namespace true` |
| Container image pull fails on Batch node | Missing `AcrPull` on the pool identity, or `containerConfiguration` not set | Re-run Step 5b; verify pool has `containerConfiguration.type = DockerCompatible` |
| `DefaultAzureCredential` picks wrong identity | Multiple managed identities on the Batch node | Set `AZURE_CLIENT_ID` env var on the pool (Step 9a) |
| Low-priority node evicted mid-task | Azure reclaimed the spot node | ADF/Batch retries automatically; pipeline is idempotent so a re-run is safe |
| Pool stays at 0 nodes after ADF submits task | Auto-scale evaluation interval has not elapsed yet | Wait up to 5 minutes for the pool to scale up; normal behaviour on first run |
| Azure Data Factory cannot submit Batch jobs | Missing `Contributor` on the Azure Batch account for the factory managed identity | Re-run Step 8a |
| Snowflake `COPY INTO` access denied | Consent or storage RBAC incomplete | Open `AZURE_CONSENT_URL` from `DESC INTEGRATION` in a browser; complete Step 11b as a Cloud Application Administrator or Global Administrator |
