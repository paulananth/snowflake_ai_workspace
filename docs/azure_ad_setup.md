# Azure AD / Entra ID Setup — SEC EDGAR Platform

Step-by-step guide to create all Managed Identities, RBAC role assignments, and Microsoft Entra ID (formerly Azure AD) configuration required to run the SEC EDGAR ingestion and transform pipeline on Azure (ADF + Azure Batch + ADLS Gen2).

---

## Prerequisites

- Azure subscription with Owner or User Access Administrator + Contributor rights
- Azure CLI installed and authenticated: `az login`
- Decisions made:
  - Resource group name (replace `{RG}` throughout)
  - Azure region (replace `{LOCATION}` — e.g. `eastus`)
  - Storage account name (replace `{STORAGE_ACCOUNT}` — must be globally unique)
  - Container name (replace `{CONTAINER}` — e.g. `sec-edgar`)
  - Storage key prefix (replace `{PREFIX}` — e.g. `sec-edgar`)
  - Azure Batch account name (replace `{BATCH_ACCOUNT}`)
  - Azure Container Registry name (replace `{ACR_NAME}`)
  - ADF instance name (replace `{ADF_NAME}`)

---

## Step 0 — Set Shell Variables

Run these once in your terminal. Every command below uses them.

```bash
SUBSCRIPTION=$(az account show --query id --output tsv)
RG=my-sec-edgar-rg
LOCATION=eastus
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

## Step 2 — Create the ADLS Gen2 Storage Account

**Hierarchical Namespace must be enabled** — this activates ADLS Gen2 and is required for the `abfss://` scheme used in the pipeline.

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
  --allow-blob-public-access false

# Create the container (filesystem in ADLS Gen2 terminology)
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

---

## Step 3 — Create the Azure Container Registry (ACR)

Stores the Docker image used by Azure Batch pool nodes.

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

**The `CLIENT_ID` value** must be set as `AZURE_CLIENT_ID` environment variable on the Batch pool (Step 8). This tells `DefaultAzureCredential` which managed identity to use.

---

## Step 5 — Grant RBAC Roles to the Managed Identity

### 5a. Storage Blob Data Contributor (ADLS Gen2)

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

echo "Batch account resource ID:"
az batch account show --name $BATCH_ACCOUNT --resource-group $RG --query id --output tsv
```

---

## Step 7 — Create the ADF Instance

```bash
az datafactory create \
  --factory-name $ADF_NAME \
  --resource-group $RG \
  --location $LOCATION

# Get ADF's system-assigned managed identity principal ID
ADF_PRINCIPAL=$(az datafactory show \
  --factory-name $ADF_NAME --resource-group $RG \
  --query identity.principalId --output tsv)

echo "ADF Principal ID: $ADF_PRINCIPAL"
```

---

## Step 8 — Grant ADF's Identity Access to Batch and Storage

ADF uses its own system-assigned managed identity to connect to Batch (to submit jobs) and to Storage (to pass scripts to the Batch pool).

### 8a. Contributor on the Batch account

Allows ADF to create and monitor Batch jobs.

```bash
BATCH_SCOPE=$(az batch account show \
  --name $BATCH_ACCOUNT --resource-group $RG --query id --output tsv)

az role assignment create \
  --role "Contributor" \
  --assignee-object-id $ADF_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --scope $BATCH_SCOPE
```

### 8b. Storage Blob Data Reader on the storage account

Allows ADF to read script files from the storage container when setting up Custom Activity tasks.

```bash
az role assignment create \
  --role "Storage Blob Data Reader" \
  --assignee-object-id $ADF_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --scope $STORAGE_SCOPE
```

---

## Step 9 — Create and Configure the Batch Pool

The pool must have `containerConfiguration` enabled so nodes can run Docker containers. The User-Assigned Managed Identity from Step 4 is attached to the pool.

```bash
# First, log in to Batch to get a management token
az batch account login \
  --name $BATCH_ACCOUNT \
  --resource-group $RG

# Create the pool with container support
az batch pool create \
  --id sec-edgar-pool \
  --account-name $BATCH_ACCOUNT \
  --vm-size Standard_D4s_v3 \
  --target-dedicated-nodes 1 \
  --image "microsoft-dsvm:ubuntu-hpc:2204:latest" \
  --node-agent-sku-id "batch.node.ubuntu 22.04" \
  --identity $IDENTITY_ID
```

> **Note:** The full `containerConfiguration` block (specifying the ACR image and container registry credentials) is set in the pool's JSON definition. Use the Azure portal or an ARM template to set `containerConfiguration.type = DockerCompatible` and add the ACR login server under `containerRegistries` with `identityReference` pointing to `$IDENTITY_ID`.

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

## Step 10 — Push Docker Image to ACR

```bash
# Build and push the pipeline image
az acr login --name $ACR_NAME

ACR_SERVER=$(az acr show --name $ACR_NAME --query loginServer --output tsv)

docker build -t ${ACR_SERVER}/sec-edgar-ingest:latest .
docker push ${ACR_SERVER}/sec-edgar-ingest:latest

echo "Image URI: ${ACR_SERVER}/sec-edgar-ingest:latest"
```

---

## Step 11 (Path B only) — Snowflake Azure Storage Integration

Skip this step if using Path A (DuckDB).

Snowflake reads bronze Parquet from ADLS Gen2 via a Storage Integration. After running `CREATE STORAGE INTEGRATION` in Snowflake and running `DESC INTEGRATION sec_edgar_adls_int`, Snowflake provides a `AZURE_CONSENT_URL` — open it in a browser to complete the Entra ID consent flow.

### 11a. Prepare the Storage Integration in Snowflake

```sql
-- Run as ACCOUNTADMIN in Snowflake
CREATE STORAGE INTEGRATION sec_edgar_adls_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER}/{PREFIX}/bronze/'
  );

DESC INTEGRATION sec_edgar_adls_int;
-- Note the AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME values
```

### 11b. Grant Snowflake's App Access to ADLS Gen2

After opening `AZURE_CONSENT_URL` in a browser (signs Snowflake's multi-tenant app into your tenant):

```bash
# Find the enterprise app object ID Snowflake created in your tenant
SNOWFLAKE_APP_NAME="<AZURE_MULTI_TENANT_APP_NAME from DESC INTEGRATION>"  # e.g. "xy12345"
SNOWFLAKE_PRINCIPAL=$(az ad sp list \
  --display-name $SNOWFLAKE_APP_NAME \
  --query "[0].id" --output tsv)

# Grant Storage Blob Data Reader — Snowflake only reads Bronze Parquet
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

# Verify your user has storage access
az role assignment list \
  --assignee $(az ad signed-in-user show --query id --output tsv) \
  --scope $STORAGE_SCOPE \
  --query "[].roleDefinitionName" --output tsv

# Test storage access
az storage blob list \
  --container-name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --prefix "${PREFIX}/" \
  --auth-mode login
```

If you need blob access as yourself (local dev), assign `Storage Blob Data Contributor` to your own user:

```bash
MY_PRINCIPAL=$(az ad signed-in-user show --query id --output tsv)

az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id $MY_PRINCIPAL \
  --assignee-principal-type User \
  --scope $STORAGE_SCOPE
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

echo "=== ADF identity ==="
az role assignment list \
  --assignee $ADF_PRINCIPAL \
  --query "[].{Role:roleDefinitionName, Scope:scope}" \
  --output table
```

Expected output for managed identity:
```
Role                         Scope
---------------------------  -----------------------------------------------
Storage Blob Data Contributor  .../storageAccounts/{STORAGE_ACCOUNT}
AcrPull                        .../registries/{ACR_NAME}
```

Expected output for ADF identity:
```
Role         Scope
-----------  -----------------------------------------------
Contributor  .../batchAccounts/{BATCH_ACCOUNT}
Storage Blob Data Reader  .../storageAccounts/{STORAGE_ACCOUNT}
```

### Confirm HNS is enabled on storage account

```bash
az storage account show \
  --name $STORAGE_ACCOUNT --resource-group $RG \
  --query isHnsEnabled --output tsv
# Expected: true
```

### Confirm ACR image is accessible from the Batch identity

```bash
# Run a test task on the Batch pool — it should pull the image without errors
az batch task create \
  --job-id test-job \
  --task-id test-pull \
  --command-line "/bin/bash -c 'echo image pulled OK'" \
  --account-name $BATCH_ACCOUNT
```

### Test blob write from local machine

```bash
echo "test" > /tmp/test.txt
az storage blob upload \
  --container-name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --name "${PREFIX}/test.txt" \
  --file /tmp/test.txt \
  --auth-mode login
# Expected: upload succeeds; then delete it:
az storage blob delete \
  --container-name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --name "${PREFIX}/test.txt" \
  --auth-mode login
```

---

## Summary — Resource IDs to Record

Copy these values into your ADF linked service definitions, pipeline parameters, and `config/settings.py`:

| Resource | Value |
|---|---|
| Storage account name | `{STORAGE_ACCOUNT}` |
| Container name | `{CONTAINER}` |
| ADLS Gen2 URL | `abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{PREFIX}` |
| Managed identity client ID (`AZURE_CLIENT_ID`) | Output of Step 4 |
| Managed identity resource ID | Output of Step 4 |
| ACR login server | `{ACR_NAME}.azurecr.io` |
| Docker image URI | `{ACR_NAME}.azurecr.io/sec-edgar-ingest:latest` |
| ADF name | `{ADF_NAME}` |
| Batch account name | `{BATCH_ACCOUNT}` |
| Batch pool ID | `sec-edgar-pool` |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `AuthorizationPermissionMismatch` when writing blobs | Missing `Storage Blob Data Contributor` on the identity | Re-run Step 5a; wait up to 5 minutes for RBAC propagation |
| `abfss://` path not found | Hierarchical Namespace not enabled on the storage account | Cannot be enabled after creation — create a new account with `--enable-hierarchical-namespace true` |
| Container image pull fails on Batch node | Missing `AcrPull` on the pool identity, or `containerConfiguration` not set | Re-run Step 5b; verify pool has `containerConfiguration.type = DockerCompatible` |
| `DefaultAzureCredential` picks wrong identity | Multiple managed identities on the Batch node | Set `AZURE_CLIENT_ID` env var on the pool (Step 9a) |
| ADF cannot submit Batch jobs | Missing `Contributor` on Batch account for ADF identity | Re-run Step 8a |
| Snowflake COPY INTO access denied | RBAC consent not completed | Open `AZURE_CONSENT_URL` from `DESC INTEGRATION` output in a browser as a Global Admin |
