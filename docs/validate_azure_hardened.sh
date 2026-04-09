#!/usr/bin/env bash
# validate_azure_hardened.sh - SEC EDGAR Azure validation

set -euo pipefail

STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-mysecedgarstorage}"
CONTAINER="${AZURE_CONTAINER:-sec-edgar}"
PREFIX="${STORAGE_PREFIX:-sec-edgar}"
BATCH_ACCOUNT="${AZURE_BATCH_ACCOUNT:-mysecedgarbatch}"
BATCH_POOL_ID="${AZURE_BATCH_POOL_ID:-sec-edgar-pool}"
ACR_NAME="${AZURE_ACR_NAME:-mysecedgaracr}"
ADF_NAME="${AZURE_DATA_FACTORY_NAME:-mysecedgaradf}"
RG="${AZURE_RESOURCE_GROUP:-my-sec-edgar-rg}"
MANAGED_IDENTITY="${AZURE_MANAGED_IDENTITY_NAME:-sec-edgar-ingest-identity}"
PIPELINE_NAME="${ADF_PIPELINE_NAME:-sec-edgar-bronze-ingest}"
TRIGGER_NAME="${ADF_TRIGGER_NAME:-DailyBronzeIngestTrigger}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
PASS=0
FAIL=0
WARN=0

ok() { echo -e "  ${GREEN}PASS${NC}  $1"; PASS=$((PASS + 1)); }
fail() { echo -e "  ${RED}FAIL${NC}  $1"; FAIL=$((FAIL + 1)); }
warn() { echo -e "  ${YELLOW}WARN${NC}  $1"; WARN=$((WARN + 1)); }
hdr() { echo; echo "-- $1 --"; }

az_q() {
  az "$@" --only-show-errors
}

try_tsv() {
  az "$@" --only-show-errors -o tsv 2>/dev/null || true
}

hdr "0. Login"
if ! az account show --query id -o tsv >/dev/null 2>&1; then
  echo "Not logged in. Run: az login"
  exit 1
fi
SUBSCRIPTION="$(az_q account show --query id -o tsv)"
ACCOUNT_NAME="$(az_q account show --query name -o tsv)"
ok "Logged in - subscription: $ACCOUNT_NAME ($SUBSCRIPTION)"

hdr "1. Resource Group"
RG_LOCATION="$(try_tsv group show --name "$RG" --query location)"
if [[ -n "$RG_LOCATION" ]]; then
  ok "Resource group '$RG' exists (location: $RG_LOCATION)"
else
  fail "Resource group '$RG' not found"
fi

hdr "2. Storage"
STORAGE_SKU="$(try_tsv storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RG" --query sku.name)"
if [[ -z "$STORAGE_SKU" ]]; then
  fail "Storage account '$STORAGE_ACCOUNT' not found in '$RG'"
else
  ok "Storage account '$STORAGE_ACCOUNT' exists (SKU: $STORAGE_SKU)"
  [[ "$(try_tsv storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RG" --query isHnsEnabled)" == "true" ]] \
    && ok "Hierarchical Namespace enabled (ADLS Gen2)" \
    || fail "Hierarchical Namespace is not enabled"
  [[ "$(try_tsv storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RG" --query minimumTlsVersion)" == "TLS1_2" ]] \
    && ok "Minimum TLS 1.2 enforced" \
    || warn "Minimum TLS is not TLS1_2"
  [[ "$(try_tsv storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RG" --query allowBlobPublicAccess)" == "false" ]] \
    && ok "Public blob access blocked" \
    || warn "Public blob access is not blocked"
fi

if az storage fs show --name "$CONTAINER" --account-name "$STORAGE_ACCOUNT" --auth-mode login --output none --only-show-errors 2>/dev/null; then
  ok "Container '$CONTAINER' exists"
else
  fail "Container '$CONTAINER' not found"
fi

if az storage fs file show --file-system "$CONTAINER" --path "adf-resources/sec-edgar-task.zip" --account-name "$STORAGE_ACCOUNT" --auth-mode login --output none --only-show-errors 2>/dev/null; then
  ok "ADF task bundle exists at '$CONTAINER/adf-resources/sec-edgar-task.zip'"
else
  warn "ADF task bundle missing at '$CONTAINER/adf-resources/sec-edgar-task.zip'"
fi

hdr "3. Batch"
BATCH_ID="$(try_tsv batch account show --name "$BATCH_ACCOUNT" --resource-group "$RG" --query id)"
if [[ -z "$BATCH_ID" ]]; then
  fail "Batch account '$BATCH_ACCOUNT' not found"
else
  ok "Batch account '$BATCH_ACCOUNT' exists"
  az batch account login --name "$BATCH_ACCOUNT" --resource-group "$RG" --only-show-errors >/dev/null
  POOL_VM="$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query vmSize)"
  if [[ -z "$POOL_VM" ]]; then
    fail "Batch pool '$BATCH_POOL_ID' not found"
  else
    ok "Batch pool '$BATCH_POOL_ID' exists (VM: $POOL_VM)"
    [[ "${POOL_VM^^}" == "STANDARD_D2S_V3" ]] && ok "VM size is Standard_D2s_v3" || warn "VM size is '$POOL_VM'"
    [[ "$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query enableAutoScale)" == "True" ]] \
      && ok "Auto-scale enabled" || warn "Auto-scale is not enabled"
    [[ "$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query targetLowPriorityNodes)" == "0" ]] \
      && ok "Low-priority nodes disabled" || warn "Low-priority nodes are still targeted"
    PUBLISHER="$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query virtualMachineConfiguration.imageReference.publisher)"
    OFFER="$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query virtualMachineConfiguration.imageReference.offer)"
    SKU="$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query virtualMachineConfiguration.imageReference.sku)"
    [[ "$PUBLISHER" == "microsoft-dsvm" && "$OFFER" == "ubuntu-hpc" && "$SKU" == "2204" ]] \
      && ok "Pool image is microsoft-dsvm/ubuntu-hpc/2204" \
      || warn "Pool image is '$PUBLISHER/$OFFER/$SKU'"
    CONTAINER_CFG="$(try_tsv batch pool show --pool-id "$BATCH_POOL_ID" --account-name "$BATCH_ACCOUNT" --query virtualMachineConfiguration.containerConfiguration)"
    [[ -z "$CONTAINER_CFG" ]] && ok "Pool has no containerConfiguration" || fail "Pool still has containerConfiguration"
  fi
fi

hdr "4. Data Factory"
ADF_IDENTITY="$(try_tsv datafactory show --factory-name "$ADF_NAME" --resource-group "$RG" --query identity.type)"
if [[ -z "$ADF_IDENTITY" ]]; then
  fail "ADF '$ADF_NAME' not found"
else
  ok "ADF '$ADF_NAME' exists"
  [[ "$ADF_IDENTITY" == "SystemAssigned" ]] && ok "ADF has a system-assigned managed identity" || warn "ADF identity type is '$ADF_IDENTITY'"

  [[ "$(try_tsv datafactory linked-service show --factory-name "$ADF_NAME" --resource-group "$RG" --linked-service-name AzureStorageLS --query properties.type)" == "AzureBlobStorage" ]] \
    && ok "AzureStorageLS type is AzureBlobStorage" \
    || fail "AzureStorageLS type is not AzureBlobStorage"
  STORAGE_LS_HAS_CONN="$(try_tsv datafactory linked-service show --factory-name "$ADF_NAME" --resource-group "$RG" --linked-service-name AzureStorageLS --query properties.connectionString.type)"
  [[ -n "$STORAGE_LS_HAS_CONN" ]] && ok "AzureStorageLS uses connectionString auth" || fail "AzureStorageLS is missing connectionString auth"

  [[ "$(try_tsv datafactory linked-service show --factory-name "$ADF_NAME" --resource-group "$RG" --linked-service-name AzureBatchLS --query properties.type)" == "AzureBatch" ]] \
    && ok "AzureBatchLS type is AzureBatch" \
    || fail "AzureBatchLS type is not AzureBatch"
  BATCH_LS_HAS_KEY="$(try_tsv datafactory linked-service show --factory-name "$ADF_NAME" --resource-group "$RG" --linked-service-name AzureBatchLS --query properties.accessKey.type)"
  [[ -n "$BATCH_LS_HAS_KEY" ]] && ok "AzureBatchLS uses accessKey auth" || fail "AzureBatchLS is missing accessKey auth"
  [[ "$(try_tsv datafactory linked-service show --factory-name "$ADF_NAME" --resource-group "$RG" --linked-service-name AzureBatchLS --query properties.accountName)" == "$BATCH_ACCOUNT" ]] \
    && ok "AzureBatchLS accountName matches '$BATCH_ACCOUNT'" \
    || warn "AzureBatchLS accountName does not match '$BATCH_ACCOUNT'"
  [[ "$(try_tsv datafactory linked-service show --factory-name "$ADF_NAME" --resource-group "$RG" --linked-service-name AzureBatchLS --query properties.poolName)" == "$BATCH_POOL_ID" ]] \
    && ok "AzureBatchLS poolName matches '$BATCH_POOL_ID'" \
    || warn "AzureBatchLS poolName does not match '$BATCH_POOL_ID'"

  PIPELINE_KIND="$(try_tsv datafactory pipeline show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$PIPELINE_NAME" --query name)"
  if [[ -z "$PIPELINE_KIND" ]]; then
    fail "Pipeline '$PIPELINE_NAME' not found"
  else
    ok "Pipeline '$PIPELINE_NAME' exists"
    for ACTIVITY in IngestTickersExchange IngestSubmissions IngestCompanyFacts; do
      [[ "$(try_tsv datafactory pipeline show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$PIPELINE_NAME" --query "activities[?name=='$ACTIVITY'].linkedServiceName.referenceName | [0]")" == "AzureBatchLS" ]] \
        && ok "$ACTIVITY uses AzureBatchLS" \
        || fail "$ACTIVITY does not reference AzureBatchLS"
      [[ "$(try_tsv datafactory pipeline show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$PIPELINE_NAME" --query "activities[?name=='$ACTIVITY'].resourceLinkedService.referenceName | [0]")" == "AzureStorageLS" ]] \
        && ok "$ACTIVITY stages resources from AzureStorageLS" \
        || fail "$ACTIVITY is missing AzureStorageLS staging"
      [[ "$(try_tsv datafactory pipeline show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$PIPELINE_NAME" --query "activities[?name=='$ACTIVITY'].folderPath | [0]")" == "$CONTAINER/adf-resources" ]] \
        && ok "$ACTIVITY folderPath is '$CONTAINER/adf-resources'" \
        || fail "$ACTIVITY folderPath is not '$CONTAINER/adf-resources'"
    done
  fi

  TRIGGER_NAME_LIVE="$(try_tsv datafactory trigger show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$TRIGGER_NAME" --query name)"
  if [[ -z "$TRIGGER_NAME_LIVE" ]]; then
    fail "Trigger '$TRIGGER_NAME' not found"
  else
    ok "Trigger '$TRIGGER_NAME' exists"
    TRIGGER_STATE="$(try_tsv datafactory trigger show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$TRIGGER_NAME" --query properties.runtimeState)"
    [[ "$TRIGGER_STATE" == "Started" ]] && ok "Trigger '$TRIGGER_NAME' is started" || warn "Trigger '$TRIGGER_NAME' runtimeState is '$TRIGGER_STATE'"
    [[ "$(try_tsv datafactory trigger show --factory-name "$ADF_NAME" --resource-group "$RG" --name "$TRIGGER_NAME" --query properties.pipeline.pipelineReference.referenceName)" == "$PIPELINE_NAME" ]] \
      && ok "Trigger targets pipeline '$PIPELINE_NAME'" \
      || fail "Trigger does not target pipeline '$PIPELINE_NAME'"
  fi
fi

hdr "5. Managed Identity RBAC"
PRINCIPAL_ID="$(try_tsv identity show --name "$MANAGED_IDENTITY" --resource-group "$RG" --query principalId)"
if [[ -z "$PRINCIPAL_ID" ]]; then
  fail "Managed identity '$MANAGED_IDENTITY' not found"
else
  ok "Managed identity '$MANAGED_IDENTITY' found"
  STORAGE_SCOPE="/subscriptions/${SUBSCRIPTION}/resourceGroups/${RG}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}"
  STORAGE_RBAC_IDS="$(try_tsv role assignment list --assignee-object-id "$PRINCIPAL_ID" --role "Storage Blob Data Contributor" --scope "$STORAGE_SCOPE" --query "[].id")"
  [[ -n "$STORAGE_RBAC_IDS" ]] \
    && ok "Storage Blob Data Contributor assigned on the storage account" \
    || fail "Storage Blob Data Contributor is missing on the storage account"
  warn "AcrPull is intentionally not required for the host-executed runtime"
fi

echo
echo "======================================"
echo "  PASS: $PASS   FAIL: $FAIL   WARN: $WARN"
echo "======================================"

if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
