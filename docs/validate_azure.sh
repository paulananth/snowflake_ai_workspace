#!/usr/bin/env bash
# validate_azure.sh — SEC EDGAR platform Azure resource validation
# Run this locally where az login works.
# Usage: bash validate_azure.sh
# Requires: Azure CLI + Reader role on subscription + Storage Blob Data Reader on storage account

set -euo pipefail

# ── CONFIGURE THESE ──────────────────────────────────────────────────────────
STORAGE_ACCOUNT=""   # e.g. mysecedgarstorage
CONTAINER=""         # e.g. sec-edgar
PREFIX=""            # e.g. sec-edgar
BATCH_ACCOUNT=""     # e.g. mysecedgarbatch
ACR_NAME=""          # e.g. mysecedgaracr
ADF_NAME=""          # e.g. mysecedgaradf
RG=""                # e.g. my-sec-edgar-rg
MANAGED_IDENTITY=""  # e.g. sec-edgar-ingest-identity
# ─────────────────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASS=0; FAIL=0; WARN=0

ok()   { echo -e "  ${GREEN}PASS${NC}  $1"; ((PASS++)); }
fail() { echo -e "  ${RED}FAIL${NC}  $1"; ((FAIL++)); }
warn() { echo -e "  ${YELLOW}WARN${NC}  $1"; ((WARN++)); }
hdr()  { echo -e "\n── $1 ──"; }

# ── 0. LOGIN CHECK ────────────────────────────────────────────────────────────
hdr "0. Login"
if ! az account show --query id --output tsv &>/dev/null; then
  echo "Not logged in. Run: az login"
  exit 1
fi
SUBSCRIPTION=$(az account show --query id --output tsv)
ACCOUNT_NAME=$(az account show --query name --output tsv)
ok "Logged in — subscription: $ACCOUNT_NAME ($SUBSCRIPTION)"

# ── 1. RESOURCE GROUP ─────────────────────────────────────────────────────────
hdr "1. Resource Group"
if az group show --name "$RG" --output none 2>/dev/null; then
  LOCATION=$(az group show --name "$RG" --query location --output tsv)
  ok "Resource group '$RG' exists (location: $LOCATION)"
else
  fail "Resource group '$RG' not found"
fi

# ── 2. STORAGE ACCOUNT ───────────────────────────────────────────────────────
hdr "2. Storage Account"
SA=$(az storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RG" \
  --query "{sku:sku.name, kind:kind, hns:isHnsEnabled, tls:minimumTlsVersion, publicAccess:allowBlobPublicAccess}" \
  --output json 2>/dev/null || echo "{}")

if [ "$SA" = "{}" ]; then
  fail "Storage account '$STORAGE_ACCOUNT' not found in '$RG'"
else
  HNS=$(echo "$SA" | python3 -c "import sys,json; print(json.load(sys.stdin).get('hns',''))")
  SKU=$(echo "$SA" | python3 -c "import sys,json; print(json.load(sys.stdin).get('sku',''))")
  TLS=$(echo "$SA" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tls',''))")
  PUB=$(echo "$SA" | python3 -c "import sys,json; print(json.load(sys.stdin).get('publicAccess',''))")

  ok "Storage account '$STORAGE_ACCOUNT' exists (SKU: $SKU)"
  [ "$HNS" = "True" ] && ok "Hierarchical Namespace enabled (ADLS Gen2)" \
    || fail "Hierarchical Namespace NOT enabled — abfss:// will not work"
  [ "$TLS" = "TLS1_2" ] && ok "Minimum TLS 1.2 enforced" \
    || warn "TLS minimum is '$TLS' (recommend TLS1_2)"
  [ "$PUB" = "False" ] && ok "Public blob access blocked" \
    || warn "Public blob access is not blocked"
fi

# ── 3. STORAGE CONTAINER ─────────────────────────────────────────────────────
hdr "3. Storage Container"
if az storage fs show --name "$CONTAINER" --account-name "$STORAGE_ACCOUNT" \
  --auth-mode login --output none 2>/dev/null; then
  ok "Container '$CONTAINER' exists"
else
  fail "Container '$CONTAINER' not found in '$STORAGE_ACCOUNT'"
fi

# ── 4. LIFECYCLE POLICY ───────────────────────────────────────────────────────
hdr "4. Lifecycle Management Policy"
POLICY=$(az storage account management-policy show \
  --account-name "$STORAGE_ACCOUNT" --resource-group "$RG" \
  --query "policy.rules[?name=='bronze-to-cool'].enabled" \
  --output tsv 2>/dev/null || echo "")
if [ "$POLICY" = "true" ]; then
  ok "Lifecycle policy 'bronze-to-cool' is active"
elif [ -z "$POLICY" ]; then
  warn "No lifecycle policy found — run Step 2a in azure_ad_setup.md to save storage cost"
else
  warn "Lifecycle policy 'bronze-to-cool' exists but is disabled"
fi

# ── 5. ACR ────────────────────────────────────────────────────────────────────
hdr "5. Azure Container Registry"
ACR=$(az acr show --name "$ACR_NAME" --resource-group "$RG" \
  --query "{sku:sku.name, loginServer:loginServer, adminEnabled:adminUserEnabled}" \
  --output json 2>/dev/null || echo "{}")

if [ "$ACR" = "{}" ]; then
  fail "ACR '$ACR_NAME' not found in '$RG'"
else
  ACR_SKU=$(echo "$ACR" | python3 -c "import sys,json; print(json.load(sys.stdin).get('sku',''))")
  ACR_ADMIN=$(echo "$ACR" | python3 -c "import sys,json; print(json.load(sys.stdin).get('adminEnabled',''))")
  ACR_SERVER=$(echo "$ACR" | python3 -c "import sys,json; print(json.load(sys.stdin).get('loginServer',''))")
  ok "ACR '$ACR_NAME' exists — login server: $ACR_SERVER (SKU: $ACR_SKU)"
  [ "$ACR_ADMIN" = "False" ] && ok "Admin user disabled (correct — using managed identity)" \
    || warn "Admin user is enabled — disable it: az acr update --name $ACR_NAME --admin-enabled false"
  ACR_IMAGE=$(az acr repository show --name "$ACR_NAME" \
    --repository sec-edgar-ingest --output tsv 2>/dev/null || echo "")
  [ -n "$ACR_IMAGE" ] && ok "Image 'sec-edgar-ingest' exists in ACR" \
    || warn "Image 'sec-edgar-ingest' not yet pushed to ACR"
fi

# ── 6. BATCH ACCOUNT ─────────────────────────────────────────────────────────
hdr "6. Azure Batch Account"
BATCH=$(az batch account show --name "$BATCH_ACCOUNT" --resource-group "$RG" \
  --query "{location:location, provisioningState:provisioningState}" \
  --output json 2>/dev/null || echo "{}")

if [ "$BATCH" = "{}" ]; then
  fail "Batch account '$BATCH_ACCOUNT' not found in '$RG'"
else
  BATCH_STATE=$(echo "$BATCH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('provisioningState',''))")
  ok "Batch account '$BATCH_ACCOUNT' exists (state: $BATCH_STATE)"

  az batch account login --name "$BATCH_ACCOUNT" --resource-group "$RG" &>/dev/null
  POOL=$(az batch pool show --pool-id sec-edgar-pool --account-name "$BATCH_ACCOUNT" \
    --query "{vmSize:vmSize, autoScale:enableAutoScale, lowPriority:targetLowPriorityNodes, dedicated:targetDedicatedNodes}" \
    --output json 2>/dev/null || echo "{}")

  if [ "$POOL" = "{}" ]; then
    warn "Batch pool 'sec-edgar-pool' not yet created"
  else
    VM=$(echo "$POOL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('vmSize',''))")
    AS=$(echo "$POOL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('autoScale',''))")
    ok "Batch pool 'sec-edgar-pool' exists (VM: $VM)"
    [ "$VM" = "standard_d2s_v3" ] && ok "VM size is Standard_D2s_v3 (cost-optimised)" \
      || warn "VM size is '$VM' — Standard_D2s_v3 recommended for cost"
    [ "$AS" = "True" ] && ok "Auto-scale enabled — pool scales to 0 when idle" \
      || warn "Auto-scale not enabled — pool may incur idle compute cost"
  fi
fi

# ── 7. ADF ────────────────────────────────────────────────────────────────────
hdr "7. Azure Data Factory"
ADF=$(az datafactory show --factory-name "$ADF_NAME" --resource-group "$RG" \
  --query "{state:provisioningState, identity:identity.type}" \
  --output json 2>/dev/null || echo "{}")

if [ "$ADF" = "{}" ]; then
  fail "Data Factory '$ADF_NAME' not found in '$RG'"
else
  ADF_STATE=$(echo "$ADF" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))")
  ADF_IDENTITY=$(echo "$ADF" | python3 -c "import sys,json; print(json.load(sys.stdin).get('identity',''))")
  ok "ADF '$ADF_NAME' exists (state: $ADF_STATE)"
  [ "$ADF_IDENTITY" = "SystemAssigned" ] && ok "ADF has system-assigned managed identity" \
    || warn "ADF managed identity type is '$ADF_IDENTITY' (expected SystemAssigned)"
fi

# ── 8. MANAGED IDENTITY RBAC ──────────────────────────────────────────────────
hdr "8. Managed Identity RBAC"
PRINCIPAL_ID=$(az identity show --name "$MANAGED_IDENTITY" --resource-group "$RG" \
  --query principalId --output tsv 2>/dev/null || echo "")

if [ -z "$PRINCIPAL_ID" ]; then
  fail "Managed identity '$MANAGED_IDENTITY' not found in '$RG'"
else
  ok "Managed identity '$MANAGED_IDENTITY' found"
  STORAGE_SCOPE="/subscriptions/${SUBSCRIPTION}/resourceGroups/${RG}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}"
  ACR_SCOPE=$(az acr show --name "$ACR_NAME" --resource-group "$RG" --query id --output tsv 2>/dev/null || echo "")

  CONTRIB=$(az role assignment list --assignee "$PRINCIPAL_ID" \
    --role "Storage Blob Data Contributor" --scope "$STORAGE_SCOPE" \
    --query "length(@)" --output tsv 2>/dev/null || echo "0")
  [ "$CONTRIB" -ge 1 ] \
    && ok "Storage Blob Data Contributor assigned on storage account" \
    || fail "Storage Blob Data Contributor NOT assigned — container writes will fail"

  if [ -n "$ACR_SCOPE" ]; then
    ACRPULL=$(az role assignment list --assignee "$PRINCIPAL_ID" \
      --role "AcrPull" --scope "$ACR_SCOPE" \
      --query "length(@)" --output tsv 2>/dev/null || echo "0")
    [ "$ACRPULL" -ge 1 ] \
      && ok "AcrPull assigned on container registry" \
      || fail "AcrPull NOT assigned — Batch pool cannot pull Docker image"
  fi
fi

# ── SUMMARY ───────────────────────────────────────────────────────────────────
echo -e "\n══════════════════════════════════════"
echo -e "  PASS: ${GREEN}${PASS}${NC}   FAIL: ${RED}${FAIL}${NC}   WARN: ${YELLOW}${WARN}${NC}"
echo "══════════════════════════════════════"
[ "$FAIL" -gt 0 ] && exit 1 || exit 0
