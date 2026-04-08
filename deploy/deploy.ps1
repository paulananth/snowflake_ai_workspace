# deploy/deploy.ps1
#
# Full Bronze layer deployment script for Windows PowerShell.
# Run from the repo root after: git pull origin claude/sec-edgar-spec-oNEcZ
#
# Usage:
#   az login
#   .\deploy\deploy.ps1
#
# Steps performed:
#   1. Set environment variables
#   2. Assign RBAC roles (idempotent)
#   3. Build and push Docker image to ACR
#   4. Validate Azure permissions
#   5. Deploy ADF linked services + pipeline + trigger via az CLI

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Resource configuration ────────────────────────────────────────────────────
$StorageAccount  = "mysecedgarstorage"
$Container       = "sec-edgar"
$Prefix          = "sec-edgar"
$BatchAccount    = "mysecedgarbatch"
$AcrName         = "mysecedgaracr"
$AdfName         = "mysecedgaradf"
$ResourceGroup   = "my-sec-edgar-rg"
$ManagedIdentity = "sec-edgar-ingest-identity"
$Region          = "eastus"

# SEC EDGAR compliance — required on every API request
$env:SEC_USER_AGENT          = "n/a prototyping paul.ananth@yahoo.com"

# Pipeline runtime env vars (picked up by config/settings.py)
$env:CLOUD_PROVIDER          = "azure"
$env:AZURE_STORAGE_ACCOUNT   = $StorageAccount
$env:AZURE_CONTAINER         = $Container
$env:STORAGE_PREFIX          = $Prefix

# RBAC setup script env vars
$env:RESOURCE_GROUP          = $ResourceGroup
$env:STORAGE_ACCOUNT         = $StorageAccount
$env:ACR_NAME                = $AcrName
$env:BATCH_ACCOUNT           = $BatchAccount
$env:ADF_NAME                = $AdfName
$env:UAMI_NAME               = $ManagedIdentity

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  SEC EDGAR Bronze Layer — Azure Deployment" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  Resource Group  : $ResourceGroup"
Write-Host "  Storage Account : $StorageAccount  (container: $Container)"
Write-Host "  ACR             : $AcrName"
Write-Host "  Batch Account   : $BatchAccount"
Write-Host "  ADF             : $AdfName"
Write-Host "  Managed Identity: $ManagedIdentity"
Write-Host "  Region          : $Region"
Write-Host "=================================================================" -ForegroundColor Cyan

# ── Step 1: Verify az login ───────────────────────────────────────────────────
Write-Host ""
Write-Host "[1/5] Verifying Azure login..." -ForegroundColor Yellow
$account = az account show --query "{name:name, id:id}" -o json | ConvertFrom-Json
if (-not $account) {
    Write-Error "Not logged in. Run: az login"
    exit 1
}
Write-Host "  OK — Subscription: $($account.name) ($($account.id))" -ForegroundColor Green

# ── Step 2: RBAC assignments ──────────────────────────────────────────────────
Write-Host ""
Write-Host "[2/5] Assigning RBAC roles..." -ForegroundColor Yellow

# Resolve resource IDs
$StorageId = az storage account show -n $StorageAccount -g $ResourceGroup --query id -o tsv
$AcrId     = az acr show -n $AcrName -g $ResourceGroup --query id -o tsv
$BatchId   = az batch account show -n $BatchAccount -g $ResourceGroup --query id -o tsv

# ADF system-assigned managed identity principal ID
$AdfMiPrincipal = az datafactory show `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --query identity.principalId -o tsv

# Batch pool UAMI principal ID
$UamiPrincipal = az identity show `
    --name $ManagedIdentity `
    --resource-group $ResourceGroup `
    --query principalId -o tsv

Write-Host "  ADF System MI  : $AdfMiPrincipal"
Write-Host "  Batch Pool UAMI: $UamiPrincipal"

function Assign-Role {
    param($Principal, $Scope, $Role, $Description)
    Write-Host "  -> $Description" -NoNewline
    $existing = az role assignment list --assignee $Principal --scope $Scope --role $Role --query "[].id" -o tsv 2>$null
    if ($existing) {
        Write-Host "  [already exists]" -ForegroundColor DarkGray
    } else {
        az role assignment create `
            --assignee-object-id $Principal `
            --assignee-principal-type ServicePrincipal `
            --role $Role `
            --scope $Scope `
            --output none
        Write-Host "  [created]" -ForegroundColor Green
    }
}

Assign-Role $AdfMiPrincipal  $BatchId   "Contributor"                  "ADF MI    → Batch    : Contributor"
Assign-Role $AdfMiPrincipal  $StorageId "Storage Blob Data Reader"      "ADF MI    → Storage  : Blob Data Reader"
Assign-Role $UamiPrincipal   $StorageId "Storage Blob Data Contributor" "Batch UAMI → Storage : Blob Data Contributor"
Assign-Role $UamiPrincipal   $AcrId     "AcrPull"                       "Batch UAMI → ACR     : AcrPull"

Write-Host "  Waiting 30s for role assignments to propagate..." -ForegroundColor DarkGray
Start-Sleep -Seconds 30

# ── Step 3: Docker build + push ───────────────────────────────────────────────
Write-Host ""
Write-Host "[3/5] Building and pushing Docker image..." -ForegroundColor Yellow
$ImageTag = "$AcrName.azurecr.io/sec-edgar-ingest:latest"

az acr login --name $AcrName
docker build -t $ImageTag .
docker push $ImageTag
Write-Host "  OK — Pushed: $ImageTag" -ForegroundColor Green

# ── Step 4: Validate permissions ──────────────────────────────────────────────
Write-Host ""
Write-Host "[4/5] Validating Azure permissions..." -ForegroundColor Yellow
uv run python scripts/validate_azure_permissions.py --cloud azure
if ($LASTEXITCODE -ne 0) {
    Write-Error "Permission validation failed. Fix the issues above before importing ADF workflows."
    exit 1
}
Write-Host "  OK — All permission checks passed" -ForegroundColor Green

# ── Step 5: Deploy ADF linked services, pipeline, trigger ────────────────────
Write-Host ""
Write-Host "[5/5] Deploying ADF pipeline..." -ForegroundColor Yellow

# Linked services (deploy individually — ADF CLI requires one at a time)
foreach ($ls in @("AzureStorageLS", "AzureBatchLS")) {
    Write-Host "  Deploying linked service: $ls"
    $lsDef = Get-Content workflows/adf_linked_services.json | ConvertFrom-Json
    $lsObj = $lsDef.linkedServices | Where-Object { $_.name -eq $ls }
    $tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
    $lsObj.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile
    az datafactory linked-service create `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --linked-service-name $ls `
        --properties "@$tmpFile" `
        --output none
    Remove-Item $tmpFile
    Write-Host "    OK" -ForegroundColor Green
}

# Pipeline
Write-Host "  Deploying pipeline: sec-edgar-bronze-ingest"
$pipelineDef = Get-Content workflows/adf_pipeline.json | ConvertFrom-Json
$tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
$pipelineDef.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile
az datafactory pipeline create `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --name "sec-edgar-bronze-ingest" `
    --pipeline "@$tmpFile" `
    --output none
Remove-Item $tmpFile
Write-Host "    OK" -ForegroundColor Green

# Trigger
Write-Host "  Deploying trigger: DailyBronzeIngestTrigger"
$triggerDef = Get-Content workflows/adf_trigger.json | ConvertFrom-Json
$tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
$triggerDef.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile
az datafactory trigger create `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --trigger-name "DailyBronzeIngestTrigger" `
    --properties "@$tmpFile" `
    --output none
Remove-Item $tmpFile

# Start the trigger
az datafactory trigger start `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --trigger-name "DailyBronzeIngestTrigger" `
    --output none
Write-Host "    OK — Trigger active" -ForegroundColor Green

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  Pipeline  : sec-edgar-bronze-ingest"
Write-Host "  Trigger   : DailyBronzeIngestTrigger (daily 06:00 UTC)"
Write-Host "  Image     : $ImageTag"
Write-Host ""
Write-Host "  To run immediately (manual trigger):"
Write-Host "    az datafactory pipeline create-run \"
Write-Host "      --factory-name $AdfName \"
Write-Host "      --resource-group $ResourceGroup \"
Write-Host "      --name sec-edgar-bronze-ingest \"
Write-Host "      --parameters '{""ingestDate"":""$(Get-Date -Format yyyy-MM-dd)""}'"
Write-Host ""
Write-Host "  Monitor runs:"
Write-Host "    https://adf.azure.com/en/monitoring/pipelineruns"
Write-Host "=================================================================" -ForegroundColor Green
