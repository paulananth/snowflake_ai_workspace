#!/usr/bin/env bash
# deploy/setup_rbac.sh
#
# Idempotent RBAC assignment for the SEC EDGAR Bronze ingest pipeline.
# Safe to re-run; duplicate assignments are silently skipped.
#
# REQUIRED env vars (set before running):
#   SUBSCRIPTION_ID     optional; if set, az account set runs first
#   RESOURCE_GROUP      e.g. rg-sec-edgar
#   STORAGE_ACCOUNT     e.g. mysecedgarstorage
#   BATCH_ACCOUNT       e.g. secedgarbatch
#   ADF_NAME            e.g. adf-sec-edgar
#   UAMI_NAME           e.g. mi-sec-edgar-batch  (User-Assigned Managed Identity for Batch pool)
#
# Usage:
#   export RESOURCE_GROUP=rg-sec-edgar
#   export STORAGE_ACCOUNT=mysecedgarstorage
#   export BATCH_ACCOUNT=secedgarbatch
#   export ADF_NAME=adf-sec-edgar
#   export UAMI_NAME=mi-sec-edgar-batch
#   bash deploy/setup_rbac.sh

set -euo pipefail

# Validate required env vars
: "${RESOURCE_GROUP:?ERROR: RESOURCE_GROUP is not set}"
: "${STORAGE_ACCOUNT:?ERROR: STORAGE_ACCOUNT is not set}"
: "${BATCH_ACCOUNT:?ERROR: BATCH_ACCOUNT is not set}"
: "${ADF_NAME:?ERROR: ADF_NAME is not set}"
: "${UAMI_NAME:?ERROR: UAMI_NAME is not set}"

if [[ -n "${SUBSCRIPTION_ID:-}" ]]; then
    az account set --subscription "$SUBSCRIPTION_ID" --only-show-errors >/dev/null
fi

echo "==================================================================="
echo "  SEC EDGAR Pipeline - RBAC Setup"
echo "==================================================================="
echo "  Resource Group  : $RESOURCE_GROUP"
echo "  Storage Account : $STORAGE_ACCOUNT"
echo "  Batch Account   : $BATCH_ACCOUNT"
echo "  ADF             : $ADF_NAME"
echo "  Batch Pool UAMI : $UAMI_NAME"
echo "==================================================================="

# Resolve resource IDs
echo ""
echo "[1/3] Resolving resource IDs..."

STORAGE_ID=$(az storage account show \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query id --output tsv \
    --only-show-errors)

BATCH_ID=$(az batch account show \
    --name "$BATCH_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query id --output tsv \
    --only-show-errors)

# ADF system-assigned managed identity principal ID
ADF_MI_PRINCIPAL=$(az datafactory show \
    --factory-name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query identity.principalId --output tsv \
    --only-show-errors)

# Batch pool UAMI principal ID (service principal backing the managed identity)
UAMI_PRINCIPAL=$(az identity show \
    --name "$UAMI_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query principalId --output tsv \
    --only-show-errors)

echo "  ADF System MI principal  : $ADF_MI_PRINCIPAL"
echo "  Batch Pool UAMI principal: $UAMI_PRINCIPAL"

# Role assignment helper. Silently skips if the assignment already exists.
assign_role() {
    local principal="$1"
    local scope="$2"
    local role="$3"
    local description="$4"

    echo ""
    echo "  -> $description"
    echo "    Role  : $role"
    echo "    Scope : $scope"

    EXISTING=$(az role assignment list \
        --assignee-object-id "$principal" \
        --scope "$scope" \
        --role "$role" \
        --query "[].id" --output tsv \
        --only-show-errors 2>/dev/null || true)

    if [[ -n "$EXISTING" ]]; then
        echo "    Status: ALREADY EXISTS (skipped)"
    else
        az role assignment create \
            --assignee-object-id "$principal" \
            --assignee-principal-type ServicePrincipal \
            --role "$role" \
            --scope "$scope" \
            --only-show-errors \
            --output none
        echo "    Status: CREATED"
    fi
}

# Identity 2: ADF System-Assigned Managed Identity
echo ""
echo "[2/3] Assigning roles to ADF System MI ($ADF_NAME)..."

assign_role \
    "$ADF_MI_PRINCIPAL" \
    "$BATCH_ID" \
    "Contributor" \
    "ADF -> Batch: submit Custom Activity tasks to the Batch pool"

# Identity 3: Batch Pool User-Assigned Managed Identity
echo ""
echo "[3/3] Assigning roles to Batch Pool UAMI ($UAMI_NAME)..."

assign_role \
    "$UAMI_PRINCIPAL" \
    "$STORAGE_ID" \
    "Storage Blob Data Contributor" \
    "Batch host runtime -> ADLS Gen2: write Bronze Parquet via adlfs + DefaultAzureCredential"

# Summary
echo ""
echo "==================================================================="
echo "  Assignment summary - storage account scope"
echo "==================================================================="
az role assignment list \
    --scope "$STORAGE_ID" \
    --query "[].{Principal:principalName, Role:roleDefinitionName, PrincipalType:principalType}" \
    --only-show-errors \
    --output table

echo ""
echo "NOTE: Role assignments can take up to 2 minutes to propagate."
echo "The current host-executed ADF runtime does not require AcrPull on the Batch pool identity."
echo "Run 'python scripts/validate_azure_permissions.py --cloud azure' to verify."
