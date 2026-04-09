# Azure Data Pipeline Setup - SEC EDGAR Platform

This document describes the current Azure deployment model for the SEC EDGAR Bronze ingest pipeline.

## Execution Modes

### Primary / validated path
- Azure Data Factory (ADF) orchestrates the run
- Azure Function Activity invokes a Python Function App
- The Function App writes Bronze Parquet to ADLS Gen2 with its system-assigned managed identity

This path is validated in the repo today for the lightweight ticker ingest stage:
- `scripts/ingest/01_ingest_tickers_exchange.py`
- `function_apps/adf_tickers_ingest/`
- `workflows/adf_linked_services_function_tickers.json`
- `workflows/adf_pipeline_function_tickers.json`
- `deploy/deploy_function_tickers.ps1`

### Fallback path
- Use Azure Batch only for stages that exceed Function timeout or memory limits
- Keep Batch as a separate execution model; do not mix it into the lightweight Function path

---

## Estimated Monthly Cost

| Resource | SKU / Config | Est. Cost/month |
|---|---|---|
| ADLS Gen2 data lake | Standard LRS, ~15 GB, Hot -> Cool lifecycle | ~$0.40 |
| Azure Function App | Flex Consumption, pay per execution | Low for daily ticker ingest |
| Function host storage | Standard LRS `StorageV2` | Low |
| Azure Data Factory | ~1-8 activity runs/day | ~$0.05-$0.25 |
| Azure Batch account | Optional fallback only | $0.00 for account |
| Azure Batch compute | Optional fallback only | Varies by region/runtime |
| Azure Container Registry | Optional / legacy | ~$5.00 only if retained |

The validated ticker-only Function path does not require Azure Batch compute or ACR.

---

## Prerequisites

- Azure subscription with permission to create resources and assign RBAC
- Azure CLI installed and authenticated: `az login`
- A globally unique ADLS Gen2 storage account name
- A globally unique Function host storage account name
- A globally unique Function App name
- An ADF factory name
- A compliant SEC contact string for `SEC_USER_AGENT`

Recommended Azure CLI prep:

```bash
az extension add --name datafactory --yes
az provider register --namespace Microsoft.Web
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.DataFactory
```

---

## Step 0 - Set Shell Variables

```bash
SUBSCRIPTION=$(az account show --query id --output tsv)
RG=my-sec-edgar-rg
LOCATION=eastus

DATA_LAKE_ACCOUNT=mysecedgarstorage
CONTAINER=sec-edgar
PREFIX=sec-edgar

SUFFIX=$(echo "$SUBSCRIPTION" | tr -d '-' | cut -c1-8)
FUNCTION_HOST_STORAGE=secedgarfn$SUFFIX
FUNCTION_APP_NAME=sec-edgar-flex-$SUFFIX

ADF_NAME=mysecedgaradf
FUNCTION_LINKED_SERVICE=AzureFunctionTickersLS
PIPELINE_NAME=sec-edgar-function-tickers-ingest

SEC_USER_AGENT="SEC EDGAR Bronze Pipeline you@example.com"
```

If you want different names, change the variables here and keep them consistent through the guide.

---

## Step 1 - Create the Resource Group

```bash
az group create \
  --name $RG \
  --location $LOCATION
```

---

## Step 2 - Create the ADLS Gen2 Data Lake

This account stores the Bronze Parquet outputs. Hierarchical Namespace must be enabled.

```bash
az storage account create \
  --name $DATA_LAKE_ACCOUNT \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --access-tier Hot

az storage fs create \
  --name $CONTAINER \
  --account-name $DATA_LAKE_ACCOUNT \
  --auth-mode login
```

Verify:

```bash
az storage account show \
  --name $DATA_LAKE_ACCOUNT \
  --resource-group $RG \
  --query isHnsEnabled \
  --output tsv
```

Expected output: `true`

---

## Step 3 - Create the Function Host Storage Account

Azure Functions needs a separate host storage account. It must be a regular `StorageV2` account without Hierarchical Namespace.

```bash
az storage account create \
  --name $FUNCTION_HOST_STORAGE \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false
```

Verify:

```bash
az storage account show \
  --name $FUNCTION_HOST_STORAGE \
  --resource-group $RG \
  --query "{kind:kind,isHnsEnabled:isHnsEnabled}" \
  --output json
```

Expected output: `kind = StorageV2`, `isHnsEnabled = false`

---

## Step 4 - Create the Azure Function App and Managed Identity

The validated repo path uses Flex Consumption with Python 3.11.

```bash
STORAGE_SCOPE="/subscriptions/${SUBSCRIPTION}/resourceGroups/${RG}/providers/Microsoft.Storage/storageAccounts/${DATA_LAKE_ACCOUNT}"

az functionapp create \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --storage-account $FUNCTION_HOST_STORAGE \
  --flexconsumption-location $LOCATION \
  --functions-version 4 \
  --runtime python \
  --runtime-version 3.11 \
  --instance-memory 2048 \
  --assign-identity [system] \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_SCOPE
```

Capture the Function identity principal ID:

```bash
FUNCTION_PRINCIPAL_ID=$(az functionapp identity show \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --query principalId \
  --output tsv)

echo "$FUNCTION_PRINCIPAL_ID"
```

The Function App writes to ADLS Gen2 through this identity.

---

## Step 5 - Create the Azure Data Factory Instance

```bash
az datafactory create \
  --factory-name $ADF_NAME \
  --resource-group $RG \
  --location $LOCATION
```

This path does not require ADF managed identity RBAC on storage or Batch. ADF calls the Function App through the Function linked service.

---

## Step 6 - Apply Function App Settings

The app settings must line up with `config/settings.py` and the Function wrapper.

```bash
az functionapp config appsettings set \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --settings \
    SEC_USER_AGENT="$SEC_USER_AGENT" \
    CLOUD_PROVIDER=azure \
    AZURE_STORAGE_ACCOUNT=$DATA_LAKE_ACCOUNT \
    AZURE_CONTAINER=$CONTAINER \
    STORAGE_PREFIX=$PREFIX
```

Verify:

```bash
az functionapp config appsettings list \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --query "[?name=='SEC_USER_AGENT' || name=='CLOUD_PROVIDER' || name=='AZURE_STORAGE_ACCOUNT' || name=='AZURE_CONTAINER' || name=='STORAGE_PREFIX'].{name:name,value:value}" \
  --output table
```

---

## Step 7 - Build and Deploy the Function Package

### Supported repo path

Use the checked-in deployment script:
- `deploy/deploy_function_tickers.ps1`

It packages:
- `function_apps/adf_tickers_ingest/`
- `config/`
- `scripts/ingest/`

It also normalizes zip entry names before deployment. This matters on Windows.

### Why the package build matters

Azure Linux Functions expects zip entry names with forward slashes. If the package is built with Windows backslashes, remote build can succeed but the runtime may still report:
- `0 functions found (Custom)`
- `No job functions found`

### Underlying Azure CLI deploy command

The actual deploy call is:

```bash
az functionapp deployment source config-zip \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --src <normalized-package.zip> \
  --build-remote true
```

### Recommended repo command

From the repo root, run:

```powershell
.\deploy\deploy_function_tickers.ps1 `
  -SubscriptionId $env:AZURE_SUBSCRIPTION_ID `
  -ResourceGroup my-sec-edgar-rg `
  -Location eastus `
  -DataStorageAccount mysecedgarstorage `
  -FunctionStorageAccount $env:AZURE_FUNCTION_STORAGE_ACCOUNT `
  -Container sec-edgar `
  -Prefix sec-edgar `
  -AdfName mysecedgaradf `
  -FunctionAppName $env:AZURE_FUNCTION_APP_NAME `
  -PipelineName sec-edgar-function-tickers-ingest `
  -FunctionLinkedServiceName AzureFunctionTickersLS `
  -IngestDate 2026-04-09
```

The script:
- deploys the Function App package
- waits for function indexing
- injects the Function host URL and function key into the ADF linked service template
- creates or updates the ADF pipeline
- can directly invoke the function for a smoke test

---

## Step 8 - Create the ADF Linked Service and Pipeline

The repo templates are:
- `workflows/adf_linked_services_function_tickers.json`
- `workflows/adf_pipeline_function_tickers.json`

### Linked service requirements

The `AzureFunction` linked service must contain:
- `functionAppUrl = https://<function-host>.azurewebsites.net`
- `functionKey` as a secure string

For Flex Consumption, read the host name from `properties.defaultHostName`:

```bash
FUNCTION_HOST=$(az functionapp show \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --query properties.defaultHostName \
  --output tsv)

echo "https://$FUNCTION_HOST"
```

Do not query `defaultHostName` at the top level for Flex. That returns the wrong shape and can leave `functionAppUrl` as `https://`, which causes:
- `Invalid URI: The hostname could not be parsed.`

### Pipeline shape

The validated pipeline is a single `AzureFunctionActivity` that POSTs:

```json
{"ingestDate":"2026-04-09"}
```

The Function activity in the template calls:
- `functionName = ingest_tickers_exchange`
- `method = POST`

---

## Step 9 - Validate the Deployment

### 9a. Confirm the function is indexed

```bash
az functionapp function list \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --output table
```

Expected output includes `ingest_tickers_exchange`

### 9b. Invoke the Function directly

```bash
FUNCTION_URL=$(az functionapp function show \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --function-name ingest_tickers_exchange \
  --query invokeUrlTemplate \
  --output tsv)

FUNCTION_KEY=$(az functionapp keys list \
  --name $FUNCTION_APP_NAME \
  --resource-group $RG \
  --query functionKeys.default \
  --output tsv)

cat >/tmp/function-run.json <<'JSON'
{"ingestDate":"2026-04-09"}
JSON

az rest \
  --method post \
  --url "${FUNCTION_URL}?code=${FUNCTION_KEY}" \
  --skip-authorization-header \
  --headers Content-Type=application/json \
  --body @/tmp/function-run.json
```

Expected response shape:

```json
{
  "status": "Succeeded",
  "ingestDate": "2026-04-09",
  "outputPath": "abfss://sec-edgar@mysecedgarstorage.dfs.core.windows.net/sec-edgar/bronze/company_tickers_exchange/ingestion_date=2026-04-09/data.parquet"
}
```

### 9c. Run the ADF pipeline

```bash
cat >/tmp/adf-run.json <<'JSON'
{"ingestDate":"2026-04-09"}
JSON

RUN_ID=$(az datafactory pipeline create-run \
  --factory-name $ADF_NAME \
  --resource-group $RG \
  --name $PIPELINE_NAME \
  --parameters @/tmp/adf-run.json \
  --query runId \
  --output tsv)

echo "$RUN_ID"
```

Check the pipeline run:

```bash
az datafactory pipeline-run show \
  --factory-name $ADF_NAME \
  --resource-group $RG \
  --run-id $RUN_ID \
  --output json
```

### 9d. Check the Bronze output

```bash
DATA_KEY=$(az storage account keys list \
  --account-name $DATA_LAKE_ACCOUNT \
  --resource-group $RG \
  --query "[0].value" \
  --output tsv)

az storage fs file show \
  --file-system $CONTAINER \
  --path "${PREFIX}/bronze/company_tickers_exchange/ingestion_date=2026-04-09/data.parquet" \
  --account-name $DATA_LAKE_ACCOUNT \
  --account-key $DATA_KEY \
  --output json
```

Expected result: file metadata is returned and the path exists.

---

## Operational Notes

- `function_apps/adf_tickers_ingest/host.json` currently sets `functionTimeout = 00:10:00`
- The validated Function path is appropriate for lightweight orchestration and sub-10-minute work
- If a stage is likely to exceed 10 minutes, uses materially more memory, or needs custom host-level dependencies, move that stage to Azure Batch
- Keep retries in the ADF Function activity for transient cold starts or short network failures

---

## Path B - Azure Batch Fallback for Heavy Stages

Use this only when a stage does not fit the Function limits.

### Minimum components
- Azure Batch account
- Batch pool on a host VM image, not a container pool
- User-assigned managed identity on the pool
- `Storage Blob Data Contributor` on the ADLS Gen2 account for the pool identity
- ADF Custom Activity pipeline and linked services

### Important constraints
- Do not rely on ADF Custom Activity to launch container tasks; it submits plain Batch tasks
- Do not configure `containerConfiguration` on the pool for this path
- Keep ACR optional / legacy unless you have a separate reason to store the image

### Minimal CLI outline

```bash
az identity create \
  --name sec-edgar-ingest-identity \
  --resource-group $RG \
  --location $LOCATION

az batch account create \
  --name mysecedgarbatch \
  --resource-group $RG \
  --location $LOCATION \
  --storage-account $DATA_LAKE_ACCOUNT
```

Then grant:
- ADF managed identity -> `Contributor` on the Batch account
- Batch UAMI -> `Storage Blob Data Contributor` on the data lake account

The legacy Batch assets remain in the repo:
- `workflows/adf_linked_services.json`
- `workflows/adf_pipeline.json`
- `workflows/adf_trigger.json`

---

## Summary - Resource Values to Record

| Resource | Value |
|---|---|
| ADLS Gen2 account | `$DATA_LAKE_ACCOUNT` |
| ADLS filesystem / container | `$CONTAINER` |
| ADLS DFS URL | `abfss://$CONTAINER@$DATA_LAKE_ACCOUNT.dfs.core.windows.net/$PREFIX` |
| Function host storage account | `$FUNCTION_HOST_STORAGE` |
| Function App name | `$FUNCTION_APP_NAME` |
| Function host URL | `https://<properties.defaultHostName>` |
| ADF factory name | `$ADF_NAME` |
| ADF Function linked service | `$FUNCTION_LINKED_SERVICE` |
| ADF ticker pipeline | `$PIPELINE_NAME` |
| Bronze ticker output path | `$PREFIX/bronze/company_tickers_exchange/ingestion_date=<date>/data.parquet` |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `0 functions found (Custom)` | The deployment zip used Windows backslashes in entry names | Build the zip with forward slashes; use `deploy/deploy_function_tickers.ps1` on Windows |
| `Invalid URI: The hostname could not be parsed.` | `functionAppUrl` in the ADF linked service was empty or malformed | For Flex, query `properties.defaultHostName` and store `https://<host>` |
| Function returns `AuthorizationPermissionMismatch` | Function App identity does not have ADLS write access | Reapply `Storage Blob Data Contributor` on the ADLS account scope |
| ADF pipeline succeeds in authoring but fails at runtime calling the Function | Wrong function key or stale linked service secret | Refresh the linked service with the current key and publish |
| `abfss://` write path fails | The data lake account does not have Hierarchical Namespace enabled | Recreate the data lake account with `--enable-hierarchical-namespace true` |
| Function stage exceeds 10 minutes | The workload is too large for the current Function host timeout | Move that stage to Premium / Dedicated Functions or Azure Batch |
| `ContainerTaskSettingsNotFound` on Batch fallback | The Batch pool was configured as a container pool but ADF submitted a plain task | Recreate the pool without `containerConfiguration` |

---

## Recommended Next Step

Use the Function path as the default Azure Bronze ingest path for lightweight stages. Keep the Batch assets isolated as fallback infrastructure for any stage that cannot reliably finish within the Function limits.
