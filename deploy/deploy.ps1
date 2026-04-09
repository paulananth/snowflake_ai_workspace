# deploy/deploy.ps1
#
# Full Bronze layer deployment script for Windows PowerShell.
# Run from the repo root after: git pull origin claude/sec-edgar-spec-oNEcZ
#
# Usage:
#   az login
#   .\deploy\deploy.ps1

[CmdletBinding()]
param(
    [string]$SubscriptionId = $(if ($env:AZURE_SUBSCRIPTION_ID) { $env:AZURE_SUBSCRIPTION_ID } else { "" }),
    [string]$ResourceGroup = $(if ($env:AZURE_RESOURCE_GROUP) { $env:AZURE_RESOURCE_GROUP } else { "my-sec-edgar-rg" }),
    [string]$StorageAccount = $(if ($env:AZURE_STORAGE_ACCOUNT) { $env:AZURE_STORAGE_ACCOUNT } else { "mysecedgarstorage" }),
    [string]$Container = $(if ($env:AZURE_CONTAINER) { $env:AZURE_CONTAINER } else { "sec-edgar" }),
    [string]$Prefix = $(if ($env:STORAGE_PREFIX) { $env:STORAGE_PREFIX } else { "sec-edgar" }),
    [string]$BatchAccount = $(if ($env:AZURE_BATCH_ACCOUNT) { $env:AZURE_BATCH_ACCOUNT } else { "mysecedgarbatch" }),
    [string]$BatchPoolId = $(if ($env:AZURE_BATCH_POOL_ID) { $env:AZURE_BATCH_POOL_ID } else { "sec-edgar-pool" }),
    [string]$AcrName = $(if ($env:AZURE_ACR_NAME) { $env:AZURE_ACR_NAME } else { "mysecedgaracr" }),
    [string]$AdfName = $(if ($env:AZURE_DATA_FACTORY_NAME) { $env:AZURE_DATA_FACTORY_NAME } else { "mysecedgaradf" }),
    [string]$ManagedIdentity = $(if ($env:AZURE_MANAGED_IDENTITY_NAME) { $env:AZURE_MANAGED_IDENTITY_NAME } else { "sec-edgar-ingest-identity" }),
    [string]$PipelineName = $(if ($env:ADF_PIPELINE_NAME) { $env:ADF_PIPELINE_NAME } else { "sec-edgar-bronze-ingest" }),
    [string]$TriggerName = $(if ($env:ADF_TRIGGER_NAME) { $env:ADF_TRIGGER_NAME } else { "DailyBronzeIngestTrigger" }),
    [string]$AzConfigDir = $(if ($env:AZURE_CONFIG_DIR) { $env:AZURE_CONFIG_DIR } else { (Join-Path (Split-Path $PSScriptRoot -Parent) ".azure-cli") }),
    [switch]$BuildLegacyDockerArtifact,
    [switch]$RefreshBatchPool,
    [switch]$ReinstallAdfObjects
)

& (Join-Path $PSScriptRoot "deploy_hardened.ps1") @PSBoundParameters
return

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Pre-flight: install required az CLI extensions silently
# ---------------------------------------------------------------------------
Write-Host "Installing required az CLI extensions..." -ForegroundColor DarkGray
# Temporarily allow non-zero exit codes so az warnings don't abort the script
$ErrorActionPreference = "Continue"
az config set extension.dynamic_install_allow_preview=true --only-show-errors | Out-Null
az extension add --name datafactory --yes --only-show-errors | Out-Null
$ErrorActionPreference = "Stop"
Write-Host "  OK" -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# Resource configuration
# ---------------------------------------------------------------------------
$StorageAccount  = "mysecedgarstorage"
$Container       = "sec-edgar"
$Prefix          = "sec-edgar"
$BatchAccount    = "mysecedgarbatch"
$BatchPoolId     = "sec-edgar-pool"
$AcrName         = "mysecedgaracr"
$AdfName         = "mysecedgaradf"
$ResourceGroup   = "my-sec-edgar-rg"
$ManagedIdentity = "sec-edgar-ingest-identity"
$StorageAccountKey = $null
$BatchAccessKey    = $null
$StorageConnectionString = $null
$ManagedIdentityId = $null
$ManagedIdentityClientId = $null
$SecUserAgent = $null
$SubscriptionId = $null

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
Write-Host "[1/4] Verifying Azure login..." -ForegroundColor Yellow
$account = az account show --query "{name:name, id:id, user:user.name}" -o json | ConvertFrom-Json
if (-not $account) {
    Write-Error "Not logged in. Run: az login"
    exit 1
}
Write-Host "  OK - Subscription: $($account.name) ($($account.id))" -ForegroundColor Green
$SubscriptionId = $account.id

$accountUser = ""
if ($account.PSObject.Properties.Name -contains "user" -and -not [string]::IsNullOrWhiteSpace($account.user)) {
    $accountUser = [string]$account.user
}

if (-not [string]::IsNullOrWhiteSpace($env:SEC_USER_AGENT)) {
    $SecUserAgent = $env:SEC_USER_AGENT.Trim()
} elseif ($accountUser -match "@") {
    $SecUserAgent = "SEC EDGAR Bronze Pipeline $accountUser"
} else {
    Write-Error "SEC_USER_AGENT is required. Set the SEC_USER_AGENT environment variable before deploy, or sign in with an Azure account that exposes a contact email."
    exit 1
}

Write-Host "  Fetching storage and batch access keys..." -NoNewline
$StorageAccountKey = az storage account keys list `
    --account-name $StorageAccount `
    --resource-group $ResourceGroup `
    --query "[0].value" `
    --output tsv `
    --only-show-errors
$BatchAccessKey = az batch account keys list `
    --name $BatchAccount `
    --resource-group $ResourceGroup `
    --query primary `
    --output tsv `
    --only-show-errors
if ([string]::IsNullOrWhiteSpace($StorageAccountKey) -or [string]::IsNullOrWhiteSpace($BatchAccessKey)) {
    Write-Host "  [FAILED]" -ForegroundColor Red
    Write-Error "Could not retrieve required access keys. Verify your Azure RBAC permits listing storage and batch keys."
    exit 1
}
$StorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=$StorageAccount;AccountKey=$StorageAccountKey;EndpointSuffix=core.windows.net"
Write-Host "  [OK]" -ForegroundColor Green

# ---------------------------------------------------------------------------
# Step 2: RBAC assignments
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[2/4] Assigning RBAC roles..." -ForegroundColor Yellow

$StorageId = az storage account show -n $StorageAccount -g $ResourceGroup --query id -o tsv
$BatchId   = az batch account show -n $BatchAccount -g $ResourceGroup --query id -o tsv

$AdfMiPrincipal = az datafactory show `
    --factory-name $AdfName `
    --resource-group $ResourceGroup `
    --query identity.principalId -o tsv

$identity = az identity show `
    --name $ManagedIdentity `
    --resource-group $ResourceGroup `
    --query "{id:id, principalId:principalId, clientId:clientId}" `
    -o json | ConvertFrom-Json
$UamiPrincipal = $identity.principalId
$ManagedIdentityId = $identity.id
$ManagedIdentityClientId = $identity.clientId

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

# ADF only needs to submit jobs to Azure Batch. The Batch pool UAMI handles
# runtime storage access from the host-executed Python process.
Assign-Role $AdfMiPrincipal $BatchId   "Contributor"                  "ADF MI     -> Batch   (Contributor)"
Assign-Role $UamiPrincipal  $StorageId "Storage Blob Data Contributor" "Batch UAMI -> Storage (Blob Data Contributor)"

Write-Host "  Waiting 30s for role assignments to propagate..." -ForegroundColor DarkGray
Start-Sleep -Seconds 30

# ---------------------------------------------------------------------------
# Step 3: Legacy Docker build + push + task bundle staging
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[3/4] Building Docker image, pushing to ACR, and staging the ADF task bundle..." -ForegroundColor Yellow
$ImageTag = "$AcrName.azurecr.io/sec-edgar-ingest:latest"
$AcrLoginServer = az acr show -n $AcrName -g $ResourceGroup --query loginServer -o tsv

az acr login --name $AcrName
docker build -t $ImageTag .
docker push $ImageTag
Write-Host "  OK - Pushed legacy image: $ImageTag" -ForegroundColor Green

Write-Host "  Uploading ADF task bundle..." -NoNewline
$bundleStage = Join-Path $env:TEMP ("sec-edgar-adf-bundle-" + [guid]::NewGuid().ToString("N"))
$bundleZip = Join-Path $env:TEMP ("sec-edgar-task-" + [guid]::NewGuid().ToString("N") + ".zip")
New-Item -ItemType Directory -Path $bundleStage | Out-Null
Copy-Item -Path ".\config" -Destination (Join-Path $bundleStage "config") -Recurse
Copy-Item -Path ".\scripts" -Destination (Join-Path $bundleStage "scripts") -Recurse
Copy-Item -Path ".\pyproject.toml" -Destination (Join-Path $bundleStage "pyproject.toml")
if (Test-Path ".\uv.lock") {
    Copy-Item -Path ".\uv.lock" -Destination (Join-Path $bundleStage "uv.lock")
}
if (Test-Path ".\.python-version") {
    Copy-Item -Path ".\.python-version" -Destination (Join-Path $bundleStage ".python-version")
}
Compress-Archive -Path (Join-Path $bundleStage "*") -DestinationPath $bundleZip -Force
az storage blob upload `
    --account-name $StorageAccount `
    --account-key $StorageAccountKey `
    --container-name $Container `
    --name "adf-resources/sec-edgar-task.zip" `
    --file $bundleZip `
    --overwrite true `
    --only-show-errors | Out-Null
Remove-Item -LiteralPath $bundleZip -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $bundleStage -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "  [OK]" -ForegroundColor Green

Write-Host "  Refreshing Batch pool host runtime..." -NoNewline
$poolAutoScaleFormula = "startingNumberOfVMs = 0; maxNumberofVMs = 1; pendingTaskSamplePercent = `$PendingTasks.GetSamplePercent(180 * TimeInterval_Second); pendingTaskSamples = pendingTaskSamplePercent < 70 ? startingNumberOfVMs : avg(`$PendingTasks.GetSample(180 * TimeInterval_Second)); `$TargetDedicatedNodes = min(maxNumberofVMs, pendingTaskSamples); `$TargetLowPriorityNodes = 0; `$NodeDeallocationOption = taskcompletion;"
$poolUri = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Batch/batchAccounts/$BatchAccount/pools/$($BatchPoolId)?api-version=2025-06-01"
$poolDefinition = @{
    identity = @{
        type = "UserAssigned"
        userAssignedIdentities = @{
            $ManagedIdentityId = @{}
        }
    }
    properties = @{
        vmSize = "STANDARD_D2S_V3"
        deploymentConfiguration = @{
            virtualMachineConfiguration = @{
                imageReference = @{
                    publisher = "microsoft-dsvm"
                    offer     = "ubuntu-hpc"
                    sku       = "2204"
                    version   = "latest"
                }
                nodeAgentSkuId = "batch.node.ubuntu 22.04"
            }
        }
        scaleSettings = @{
            autoScale = @{
                evaluationInterval = "PT5M"
                formula            = $poolAutoScaleFormula
            }
        }
        taskSchedulingPolicy = @{
            nodeFillType = "Spread"
        }
        taskSlotsPerNode = 1
    }
}
$poolFile = [System.IO.Path]::GetTempFileName() + ".json"
$poolDefinition | ConvertTo-Json -Depth 30 | Set-Content $poolFile -Encoding UTF8
try {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    az rest `
        --method delete `
        --uri $poolUri `
        --only-show-errors 2>$null | Out-Null

    for ($attempt = 0; $attempt -lt 36; $attempt++) {
        az rest `
            --method get `
            --uri $poolUri `
            --output none `
            --only-show-errors 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            break
        }
        Start-Sleep -Seconds 5
    }
    $ErrorActionPreference = $previousErrorActionPreference

    az rest `
        --method put `
        --uri $poolUri `
        --body "@$poolFile" `
        --only-show-errors | Out-Null
} finally {
    $ErrorActionPreference = "Stop"
    Remove-Item $poolFile -Force -ErrorAction SilentlyContinue
}
Write-Host "  [OK]" -ForegroundColor Green

# ---------------------------------------------------------------------------
# Step 4: Deploy ADF linked services, pipeline, trigger
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[4/4] Deploying ADF pipeline..." -ForegroundColor Yellow

function Invoke-Az {
    param([string]$Description, [scriptblock]$Command)
    Write-Host "  $Description" -NoNewline
    $output = & $Command 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [FAILED]" -ForegroundColor Red
        Write-Host "    $output" -ForegroundColor Red
        throw "az command failed: $Description"
    }
    Write-Host "  [OK]" -ForegroundColor Green
}

function Test-AdfTriggerExists {
    param([string]$TriggerName)
    $count = az datafactory trigger list `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --query "[?name=='$TriggerName'] | length(@)" `
        --output tsv `
        --only-show-errors
    return ($LASTEXITCODE -eq 0 -and [int]$count -gt 0)
}

function Test-AdfPipelineExists {
    param([string]$PipelineName)
    $count = az datafactory pipeline list `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --query "[?name=='$PipelineName'] | length(@)" `
        --output tsv `
        --only-show-errors
    return ($LASTEXITCODE -eq 0 -and [int]$count -gt 0)
}

function Wait-AdfPipelineDeleted {
    param([string]$PipelineName)
    for ($attempt = 0; $attempt -lt 12; $attempt++) {
        if (-not (Test-AdfPipelineExists $PipelineName)) {
            return
        }
        Start-Sleep -Seconds 5
    }
    throw "Timed out waiting for pipeline deletion: $PipelineName"
}

function Convert-ToAdfLiteral([string]$Value) {
    return $Value.Replace("'", "''")
}

function New-AdfHostCommandExpression([string]$ScriptPath) {
    $bootstrap = "set -euo pipefail; python3 -m pip --version >/dev/null 2>&1 || python3 -m ensurepip --upgrade; python3 -m pip install --user uv >/dev/null; export PATH=""$HOME/.local/bin:$PATH""; python3 -c ""import zipfile; zipfile.ZipFile('sec-edgar-task.zip').extractall('app')""; cd app; uv sync --no-dev; SEC_USER_AGENT=""$SecUserAgent"" CLOUD_PROVIDER=azure AZURE_STORAGE_ACCOUNT=$StorageAccount AZURE_CONTAINER=$Container STORAGE_PREFIX=$Prefix AZURE_CLIENT_ID=$ManagedIdentityClientId .venv/bin/python $ScriptPath --date "
    $prefix = "/bin/bash -lc '$bootstrap"
    $suffix = "'"
    $prefixLiteral = Convert-ToAdfLiteral $prefix
    $suffixLiteral = Convert-ToAdfLiteral $suffix
    return "@concat('$prefixLiteral', pipeline().parameters.ingestDate, '$suffixLiteral')"
}

$PipelineName = "sec-edgar-bronze-ingest"
$TriggerName  = "DailyBronzeIngestTrigger"

# Reinstall trigger + pipeline on every deploy to avoid stale ADF metadata.
if (Test-AdfTriggerExists $TriggerName) {
    $triggerState = az datafactory trigger list `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --query "[?name=='$TriggerName'].properties.runtimeState | [0]" `
        --output tsv `
        --only-show-errors

    if ($LASTEXITCODE -eq 0 -and $triggerState -eq "Started") {
        Invoke-Az "Trigger: $TriggerName (stop before reinstall)" {
            az datafactory trigger stop `
                --factory-name $AdfName `
                --resource-group $ResourceGroup `
                --name $TriggerName `
                --only-show-errors
        }
    }

    Invoke-Az "Trigger: $TriggerName (delete before reinstall)" {
        az datafactory trigger delete `
            --factory-name $AdfName `
            --resource-group $ResourceGroup `
            --name $TriggerName `
            --yes `
            --only-show-errors
    }

    Invoke-Az "Trigger: $TriggerName (wait for delete)" {
        az datafactory trigger wait `
            --factory-name $AdfName `
            --resource-group $ResourceGroup `
            --name $TriggerName `
            --deleted `
            --interval 5 `
            --timeout 120 `
            --only-show-errors
    }
}

if (Test-AdfPipelineExists $PipelineName) {
    Invoke-Az "Pipeline: $PipelineName (delete before reinstall)" {
        az datafactory pipeline delete `
            --factory-name $AdfName `
            --resource-group $ResourceGroup `
            --name $PipelineName `
            --yes `
            --only-show-errors
    }
    Wait-AdfPipelineDeleted $PipelineName
    Write-Host "  Pipeline removed - ready for reinstall" -ForegroundColor DarkGray
}

# Linked services - AzureStorageLS must be deployed before AzureBatchLS (it references it)
$lsDef = Get-Content workflows/adf_linked_services.json -Raw | ConvertFrom-Json
foreach ($ls in @("AzureStorageLS", "AzureBatchLS")) {
    $lsObj = $lsDef.linkedServices | Where-Object { $_.name -eq $ls }
    if ($ls -eq "AzureStorageLS") {
        $null = $lsObj.properties.typeProperties.PSObject.Properties.Remove("serviceEndpoint")
        $null = $lsObj.properties.typeProperties.PSObject.Properties.Remove("authenticationType")
        $null = $lsObj.properties.typeProperties.PSObject.Properties.Remove("accountKey")
        $lsObj.properties.typeProperties.connectionString = [pscustomobject]@{
            type  = "SecureString"
            value = $StorageConnectionString
        }
    }
    if ($ls -eq "AzureBatchLS") {
        $null = $lsObj.properties.typeProperties.PSObject.Properties.Remove("authentication")
        $lsObj.properties.typeProperties.accessKey = [pscustomobject]@{
            type  = "SecureString"
            value = $BatchAccessKey
        }
    }
    $tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
    $lsObj.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile -Encoding UTF8
    Invoke-Az "Linked service: $ls" {
        az datafactory linked-service create `
            --factory-name $AdfName `
            --resource-group $ResourceGroup `
            --linked-service-name $ls `
            --properties "@$tmpFile" `
            --only-show-errors
    }
    Remove-Item $tmpFile
}

# Pipeline
$tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
$pipelineDef = Get-Content workflows/adf_pipeline.json -Raw | ConvertFrom-Json
$commandScripts = @{
    "IngestTickersExchange" = "scripts/ingest/01_ingest_tickers_exchange.py"
    "IngestSubmissions"     = "scripts/ingest/02_ingest_submissions.py"
    "IngestCompanyFacts"    = "scripts/ingest/03_ingest_companyfacts.py"
}
foreach ($activity in $pipelineDef.properties.activities) {
    if ($commandScripts.ContainsKey($activity.name)) {
        $activity.typeProperties.resourceLinkedService = [pscustomobject]@{
            referenceName = "AzureStorageLS"
            type = "LinkedServiceReference"
        }
        $activity.typeProperties.folderPath = "$Container/adf-resources"
        $activity.typeProperties.referenceObjects = [pscustomobject]@{
            linkedServices = @()
            datasets = @()
        }
        $activity.typeProperties.command.type = "Expression"
        $activity.typeProperties.command.value = New-AdfHostCommandExpression $commandScripts[$activity.name]
    }
}
$pipelineDef.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile -Encoding UTF8
Invoke-Az "Pipeline: sec-edgar-bronze-ingest" {
    az datafactory pipeline create `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --name $PipelineName `
        --pipeline "@$tmpFile" `
        --only-show-errors
}
Remove-Item $tmpFile

# Trigger
$tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
$triggerDef = Get-Content workflows/adf_trigger.json -Raw | ConvertFrom-Json
$triggerDef.properties | ConvertTo-Json -Depth 20 | Set-Content $tmpFile -Encoding UTF8
Invoke-Az "Trigger: DailyBronzeIngestTrigger (create)" {
    az datafactory trigger create `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --name $TriggerName `
        --properties "@$tmpFile" `
        --only-show-errors
}
Remove-Item $tmpFile

Invoke-Az "Trigger: DailyBronzeIngestTrigger (start)" {
    az datafactory trigger start `
        --factory-name $AdfName `
        --resource-group $ResourceGroup `
        --name $TriggerName `
        --only-show-errors
}
Write-Host "  Trigger active - fires daily at 06:00 UTC" -ForegroundColor Green

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
Write-Host "  Task zip : $Container/adf-resources/sec-edgar-task.zip"
Write-Host "  Runtime  : Azure Batch host VM (non-container pool)"
Write-Host "  Image    : $ImageTag (legacy build artifact)"
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
