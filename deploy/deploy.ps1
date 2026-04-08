# deploy/deploy.ps1
#
# Full Bronze layer deployment script for Windows PowerShell.
# Run from the repo root after: git pull origin claude/sec-edgar-spec-oNEcZ
#
# Usage:
#   az login
#   .\deploy\deploy.ps1

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Pre-flight: install required az CLI extensions silently
# ---------------------------------------------------------------------------
Write-Host "Installing required az CLI extensions..." -ForegroundColor DarkGray
# Temporarily allow non-zero exit codes so az warnings don't abort the script
$ErrorActionPreference = "Continue"
az config set extension.dynamic_install_allow_preview=true --only-show-errors | Out-Null
az extension add --name datafactory --yes --only-show-errors | Out-Null
az extension add --name azure-batch  --yes --only-show-errors | Out-Null
$ErrorActionPreference = "Stop"
Write-Host "  OK" -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# Resource configuration
# ---------------------------------------------------------------------------
$StorageAccount  = "mysecedgarstorage"
$Container       = "sec-edgar"
$Prefix          = "sec-edgar"
$BatchAccount    = "mysecedgarbatch"
$AcrName         = "mysecedgaracr"
$AdfName         = "mysecedgaradf"
$ResourceGroup   = "my-sec-edgar-rg"
$ManagedIdentity = "sec-edgar-ingest-identity"

# SEC EDGAR compliance - required on every API request
$env:SEC_USER_AGENT        = "n/a prototyping paul.ananth@yahoo.com"

# Pipeline runtime env vars (read by config/settings.py)
$env:CLOUD_PROVIDER        = "azure"
$env:AZURE_STORAGE_ACCOUNT = $StorageAccount
$env:AZURE_CONTAINER       = $Container
$env:STORAGE_PREFIX        = $Prefix

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  SEC EDGAR Bronze Layer - Azure Deployment" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  Resource Group   = $ResourceGroup"
Write-Host "  Storage Account  = $StorageAccount (container=$Container)"
Write-Host "  ACR              = $AcrName"
Write-Host "  Batch Account    = $BatchAccount"
Write-Host "  ADF              = $AdfName"
Write-Host "  Managed Identity = $ManagedIdentity"
Write-Host "=================================================================" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# Step 1: Verify az login
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[1/5] Verifying Azure login..." -ForegroundColor Yellow
$account = az account show --query "{name:name, id:id}" -o json | ConvertFrom-Json
if (-not $account) {
    Write-Error "Not logged in. Run: az login"
    exit 1
}
Write-Host "  OK - Subscription: $($account.name) ($($account.id))" -ForegroundColor Green

# ---------------------------------------------------------------------------
# Step 2: RBAC assignments
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[2/5] Assigning RBAC roles..." -ForegroundColor Yellow

$StorageId = az storage account show -n $StorageAccount -g $ResourceGroup --query id -o tsv
$AcrId     = az acr show -n $AcrName -g $ResourceGroup --query id -o tsv
$BatchId   = az batch account show -n $BatchAccount -g $ResourceGroup --query id -o tsv

$AdfMiPrincipal = az datafactory show `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --query identity.principalId -o tsv

$UamiPrincipal = az identity show `
    --name $ManagedIdentity `
    --resource-group $ResourceGroup `
    --query principalId -o tsv

Write-Host "  ADF System MI   = $AdfMiPrincipal"
Write-Host "  Batch Pool UAMI = $UamiPrincipal"

function Assign-Role($Principal, $Scope, $Role, $Description) {
    Write-Host "  -> $Description" -NoNewline
    $existing = az role assignment list --assignee $Principal --scope $Scope `
        --role $Role --query "[].id" -o tsv 2>$null
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

Assign-Role $AdfMiPrincipal $BatchId   "Contributor"                  "ADF MI     -> Batch   (Contributor)"
Assign-Role $AdfMiPrincipal $StorageId "Storage Blob Data Reader"      "ADF MI     -> Storage (Blob Data Reader)"
Assign-Role $UamiPrincipal  $StorageId "Storage Blob Data Contributor" "Batch UAMI -> Storage (Blob Data Contributor)"
Assign-Role $UamiPrincipal  $AcrId     "AcrPull"                       "Batch UAMI -> ACR     (AcrPull)"

Write-Host "  Waiting 30s for role assignments to propagate..." -ForegroundColor DarkGray
Start-Sleep -Seconds 30

# ---------------------------------------------------------------------------
# Step 3: Docker build + push
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[3/5] Building and pushing Docker image..." -ForegroundColor Yellow
$ImageTag = "$AcrName.azurecr.io/sec-edgar-ingest:latest"

az acr login --name $AcrName
docker build -t $ImageTag .
docker push $ImageTag
Write-Host "  OK - Pushed: $ImageTag" -ForegroundColor Green

# ---------------------------------------------------------------------------
# Step 4: Validate permissions
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[4/5] Validating Azure permissions..." -ForegroundColor Yellow
uv run python scripts/validate_azure_permissions.py --cloud azure
if ($LASTEXITCODE -ne 0) {
    Write-Error "Permission validation failed. Fix the issues above before deploying ADF."
    exit 1
}
Write-Host "  OK - All permission checks passed" -ForegroundColor Green

# ---------------------------------------------------------------------------
# Step 5: Deploy ADF linked services, pipeline, trigger
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[5/5] Deploying ADF pipeline..." -ForegroundColor Yellow

# Linked services - deploy AzureStorageLS first (AzureBatchLS references it)
foreach ($ls in @("AzureStorageLS", "AzureBatchLS")) {
    Write-Host "  Deploying linked service: $ls"
    $lsDef = Get-Content workflows/adf_linked_services.json -Raw | ConvertFrom-Json
    $lsObj = $lsDef.linkedServices | Where-Object { $_.name -eq $ls }
    $tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
    $lsObj.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile -Encoding UTF8
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
$tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
$pipelineDef = Get-Content workflows/adf_pipeline.json -Raw | ConvertFrom-Json
$pipelineDef.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile -Encoding UTF8
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
$tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
$triggerDef = Get-Content workflows/adf_trigger.json -Raw | ConvertFrom-Json
$triggerDef.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile -Encoding UTF8
az datafactory trigger create `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --trigger-name "DailyBronzeIngestTrigger" `
    --properties "@$tmpFile" `
    --output none
Remove-Item $tmpFile

az datafactory trigger start `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --trigger-name "DailyBronzeIngestTrigger" `
    --output none
Write-Host "    OK - Trigger active (fires daily at 06:00 UTC)" -ForegroundColor Green

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
$Today = Get-Date -Format "yyyy-MM-dd"
Write-Host ""
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  Pipeline : sec-edgar-bronze-ingest"
Write-Host "  Trigger  : DailyBronzeIngestTrigger (daily 06:00 UTC)"
Write-Host "  Image    : $ImageTag"
Write-Host ""
Write-Host "  To run immediately (manual):"
Write-Host "  az datafactory pipeline create-run ``"
Write-Host "    --factory-name $AdfName ``"
Write-Host "    --resource-group $ResourceGroup ``"
Write-Host "    --name sec-edgar-bronze-ingest ``"
Write-Host "    --parameters '{""ingestDate"":""$Today""}'"
Write-Host ""
Write-Host "  Monitor: https://adf.azure.com/en/monitoring/pipelineruns"
Write-Host "=================================================================" -ForegroundColor Green
