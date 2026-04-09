# deploy/deploy_hardened.ps1
#
# Idempotent Azure CLI deployment for the SEC EDGAR Bronze layer.

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

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path $PSScriptRoot -Parent
$LinkedServicesPath = Join-Path $RepoRoot "workflows\adf_linked_services.json"
$PipelinePath = Join-Path $RepoRoot "workflows\adf_pipeline.json"
$TriggerPath = Join-Path $RepoRoot "workflows\adf_trigger.json"
$TaskBundleBlobName = "adf-resources/sec-edgar-task.zip"
$TaskBundleFolderPath = "$Container/adf-resources"
$PoolImagePublisher = "microsoft-dsvm"
$PoolImageOffer = "ubuntu-hpc"
$PoolImageSku = "2204"
$PoolImageVersion = "latest"
$PoolNodeAgentSkuId = "batch.node.ubuntu 22.04"
$VmSize = "STANDARD_D2S_V3"
$PoolAutoScaleFormula = "startingNumberOfVMs = 0; maxNumberofVMs = 1; pendingTaskSamplePercent = `$PendingTasks.GetSamplePercent(180 * TimeInterval_Second); pendingTaskSamples = pendingTaskSamplePercent < 70 ? startingNumberOfVMs : avg(`$PendingTasks.GetSample(180 * TimeInterval_Second)); `$TargetDedicatedNodes = min(maxNumberofVMs, pendingTaskSamples); `$TargetLowPriorityNodes = 0; `$NodeDeallocationOption = taskcompletion;"
$LegacyImageName = "sec-edgar-ingest:latest"

$StorageAccountKey = $null
$BatchAccessKey = $null
$StorageConnectionString = $null
$ManagedIdentityId = $null
$ManagedIdentityClientId = $null
$SecUserAgent = $null
$BatchId = $null
$BatchUri = $null
$StorageId = $null
$AdfMiPrincipal = $null
$AzureCliExe = $null
$AzureCliResolved = $false
$UsePythonAzCli = $false

function Write-Section([string]$Title) {
    Write-Host ""
    Write-Host $Title -ForegroundColor Yellow
}

function Write-Status([string]$Message, [string]$Status, [ConsoleColor]$Color) {
    Write-Host ("  {0}  [{1}]" -f $Message, $Status) -ForegroundColor $Color
}

function Ensure-AzConfigDir {
    if ([string]::IsNullOrWhiteSpace($env:AZURE_CONFIG_DIR) -and -not [string]::IsNullOrWhiteSpace($AzConfigDir)) {
        New-Item -ItemType Directory -Path $AzConfigDir -Force | Out-Null
        $env:AZURE_CONFIG_DIR = $AzConfigDir
    }
}

function Resolve-AzRunner {
    if ($script:AzureCliResolved) {
        return
    }

    $azCommand = Get-Command az -ErrorAction Stop
    $script:AzureCliExe = $azCommand.Source

    $isWindowsPlatform = $env:OS -eq "Windows_NT"
    if ($isWindowsPlatform) {
        $cliRoot = Split-Path (Split-Path $azCommand.Source -Parent) -Parent
        $pythonCandidate = Join-Path $cliRoot "python.exe"
        if (Test-Path $pythonCandidate) {
            $script:AzureCliExe = $pythonCandidate
            $script:UsePythonAzCli = $true
        }
    }

    $script:AzureCliResolved = $true
}

function Invoke-Az {
    param(
        [Parameter(Mandatory = $true)][string[]]$Args,
        [switch]$Json,
        [switch]$AllowFailure
    )

    Resolve-AzRunner
    $effectiveArgs = [System.Collections.Generic.List[string]]::new()
    foreach ($arg in $Args) {
        [void]$effectiveArgs.Add($arg)
    }
    if ($effectiveArgs -notcontains "--only-show-errors") {
        [void]$effectiveArgs.Add("--only-show-errors")
    }

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        if ($script:UsePythonAzCli) {
            $output = & $script:AzureCliExe -m azure.cli @effectiveArgs 2>&1
        } else {
            $output = & $script:AzureCliExe @effectiveArgs 2>&1
        }
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $exitCode = $LASTEXITCODE
    $text = ($output | Out-String).Trim()

    if ($AllowFailure) {
        return [pscustomobject]@{
            Success = ($exitCode -eq 0)
            Output = $text
        }
    }

    if ($exitCode -ne 0) {
        throw "Azure CLI command failed: az $($effectiveArgs -join ' ')`n$text"
    }

    if ($Json) {
        if ([string]::IsNullOrWhiteSpace($text)) {
            return $null
        }
        return ($text | ConvertFrom-Json)
    }

    return $text
}

function Ensure-RequiredExtension([string]$Name) {
    $exists = Invoke-Az -Args @("extension", "show", "--name", $Name, "--output", "none") -AllowFailure
    if ($exists.Success) {
        return
    }

    az config set extension.dynamic_install_allow_preview=true --only-show-errors | Out-Null
    az extension add --name $Name --yes --only-show-errors | Out-Null
}

function Get-CanonicalObject($Value) {
    if ($null -eq $Value) {
        return $null
    }
    if ($Value -is [string] -or $Value -is [ValueType]) {
        return $Value
    }
    if ($Value -is [System.Collections.IDictionary]) {
        $ordered = [ordered]@{}
        foreach ($key in ($Value.Keys | Sort-Object)) {
            $ordered[$key] = Get-CanonicalObject $Value[$key]
        }
        return [pscustomobject]$ordered
    }
    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        $items = @()
        foreach ($item in $Value) {
            $items += ,(Get-CanonicalObject $item)
        }
        return $items
    }

    $properties = $Value.PSObject.Properties | Sort-Object Name
    if ($properties.Count -gt 0) {
        $ordered = [ordered]@{}
        foreach ($property in $properties) {
            $ordered[$property.Name] = Get-CanonicalObject $property.Value
        }
        return [pscustomobject]$ordered
    }

    return $Value
}

function Get-JsonFingerprint($Value) {
    return ((Get-CanonicalObject $Value) | ConvertTo-Json -Depth 100 -Compress)
}

function Normalize-Whitespace([string]$Value) {
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }
    return (($Value -replace "\s+", "")).Trim()
}

function Get-SecUserAgent([object]$Account) {
    if (-not [string]::IsNullOrWhiteSpace($env:SEC_USER_AGENT)) {
        return $env:SEC_USER_AGENT.Trim()
    }

    $accountUser = ""
    if ($Account.PSObject.Properties.Name -contains "user" -and -not [string]::IsNullOrWhiteSpace($Account.user)) {
        $accountUser = [string]$Account.user
    }

    if ($accountUser -match "@") {
        return "SEC EDGAR Bronze Pipeline $accountUser"
    }

    throw "SEC_USER_AGENT is required. Set SEC_USER_AGENT or sign in with an Azure account that exposes a contact email."
}

function Ensure-AzureContext {
    Write-Section "[1/4] Verifying Azure CLI context..."
    Ensure-AzConfigDir
    Ensure-RequiredExtension "datafactory"

    $account = Invoke-Az -Args @("account", "show", "--query", "{name:name,id:id,user:user.name}", "--output", "json") -Json
    if (-not $account) {
        throw "Not logged in. Run: az login"
    }

    if (-not [string]::IsNullOrWhiteSpace($SubscriptionId)) {
        Invoke-Az -Args @("account", "set", "--subscription", $SubscriptionId, "--output", "none") | Out-Null
        $account = Invoke-Az -Args @("account", "show", "--query", "{name:name,id:id,user:user.name}", "--output", "json") -Json
    } else {
        $script:SubscriptionId = [string]$account.id
    }

    $script:SecUserAgent = Get-SecUserAgent $account
    Write-Status "Subscription: $($account.name) ($($account.id))" "OK" Green
}

function Get-SecretMaterial {
    Write-Host "  Fetching linked-service credentials..." -NoNewline
    $script:StorageAccountKey = Invoke-Az -Args @(
        "storage", "account", "keys", "list",
        "--account-name", $StorageAccount,
        "--resource-group", $ResourceGroup,
        "--query", "[0].value",
        "--output", "tsv"
    )
    $script:BatchAccessKey = Invoke-Az -Args @(
        "batch", "account", "keys", "list",
        "--name", $BatchAccount,
        "--resource-group", $ResourceGroup,
        "--query", "primary",
        "--output", "tsv"
    )
    $batchEndpoint = Invoke-Az -Args @(
        "batch", "account", "show",
        "--name", $BatchAccount,
        "--resource-group", $ResourceGroup,
        "--query", "accountEndpoint",
        "--output", "tsv"
    )
    if ([string]::IsNullOrWhiteSpace($StorageAccountKey) -or [string]::IsNullOrWhiteSpace($BatchAccessKey)) {
        Write-Host "  [FAILED]" -ForegroundColor Red
        throw "Could not retrieve storage or batch access keys. Verify your Azure RBAC permits listing keys."
    }
    if ([string]::IsNullOrWhiteSpace($batchEndpoint)) {
        Write-Host "  [FAILED]" -ForegroundColor Red
        throw "Could not determine the Azure Batch account endpoint."
    }
    $script:StorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=$StorageAccount;AccountKey=$StorageAccountKey;EndpointSuffix=core.windows.net"
    if ($batchEndpoint -match "^https?://") {
        $script:BatchUri = $batchEndpoint
    } else {
        $script:BatchUri = "https://$batchEndpoint"
    }
    Write-Host "  [OK]" -ForegroundColor Green
}

function Ensure-RoleAssignment([string]$PrincipalId, [string]$Scope, [string]$Role, [string]$Label) {
    Write-Host "  -> $Label" -NoNewline
    $existing = Invoke-Az -Args @(
        "role", "assignment", "list",
        "--assignee-object-id", $PrincipalId,
        "--scope", $Scope,
        "--role", $Role,
        "--output", "json"
    ) -Json
    if (@($existing).Count -gt 0) {
        Write-Host "  [already exists]" -ForegroundColor DarkGray
        return
    }

    Invoke-Az -Args @(
        "role", "assignment", "create",
        "--assignee-object-id", $PrincipalId,
        "--assignee-principal-type", "ServicePrincipal",
        "--role", $Role,
        "--scope", $Scope,
        "--output", "none"
    ) | Out-Null
    Write-Host "  [created]" -ForegroundColor Green
}

function Ensure-Rbac {
    Write-Section "[2/4] Ensuring RBAC..."
    $script:StorageId = Invoke-Az -Args @("storage", "account", "show", "-n", $StorageAccount, "-g", $ResourceGroup, "--query", "id", "-o", "tsv")
    $script:BatchId = Invoke-Az -Args @("batch", "account", "show", "-n", $BatchAccount, "-g", $ResourceGroup, "--query", "id", "-o", "tsv")
    $script:AdfMiPrincipal = Invoke-Az -Args @(
        "datafactory", "show",
        "--factory-name", $AdfName,
        "--resource-group", $ResourceGroup,
        "--query", "identity.principalId",
        "--output", "tsv"
    )
    $identity = Invoke-Az -Args @(
        "identity", "show",
        "--name", $ManagedIdentity,
        "--resource-group", $ResourceGroup,
        "--query", "{id:id,principalId:principalId,clientId:clientId}",
        "--output", "json"
    ) -Json
    $script:ManagedIdentityId = [string]$identity.id
    $script:ManagedIdentityClientId = [string]$identity.clientId

    Write-Host "  ADF System MI   = $AdfMiPrincipal"
    Write-Host "  Batch Pool UAMI = $($identity.principalId)"

    Ensure-RoleAssignment $AdfMiPrincipal $BatchId "Contributor" "ADF MI     -> Batch   (Contributor)"
    Ensure-RoleAssignment ([string]$identity.principalId) $StorageId "Storage Blob Data Contributor" "Batch UAMI -> Storage (Blob Data Contributor)"
}

function New-TaskBundleZip {
    $bundleStage = Join-Path $env:TEMP ("sec-edgar-adf-bundle-" + [guid]::NewGuid().ToString("N"))
    $bundleZip = Join-Path $env:TEMP ("sec-edgar-task-" + [guid]::NewGuid().ToString("N") + ".zip")
    New-Item -ItemType Directory -Path $bundleStage | Out-Null

    try {
        Copy-Item -Path (Join-Path $RepoRoot "config") -Destination (Join-Path $bundleStage "config") -Recurse
        Copy-Item -Path (Join-Path $RepoRoot "scripts") -Destination (Join-Path $bundleStage "scripts") -Recurse
        Copy-Item -Path (Join-Path $RepoRoot "pyproject.toml") -Destination (Join-Path $bundleStage "pyproject.toml")
        foreach ($extra in @("uv.lock", ".python-version")) {
            $extraPath = Join-Path $RepoRoot $extra
            if (Test-Path $extraPath) {
                Copy-Item -Path $extraPath -Destination (Join-Path $bundleStage $extra)
            }
        }
        Compress-Archive -Path (Join-Path $bundleStage "*") -DestinationPath $bundleZip -Force
        return $bundleZip
    } finally {
        Remove-Item -LiteralPath $bundleStage -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Publish-LegacyDockerArtifact {
    if (-not $BuildLegacyDockerArtifact.IsPresent) {
        Write-Status "Legacy Docker artifact build disabled" "SKIPPED" DarkGray
        return
    }

    Write-Host "  Building legacy Docker artifact in ACR..." -NoNewline
    Invoke-Az -Args @(
        "acr", "build",
        "--registry", $AcrName,
        "--image", $LegacyImageName,
        $RepoRoot
    ) | Out-Null
    Write-Host "  [OK]" -ForegroundColor Green
}

function Publish-TaskBundle {
    Write-Host "  Uploading ADF task bundle..." -NoNewline
    $bundleZip = New-TaskBundleZip
    $previousStorageConnectionString = $env:AZURE_STORAGE_CONNECTION_STRING
    try {
        $env:AZURE_STORAGE_CONNECTION_STRING = $StorageConnectionString
        Invoke-Az -Args @(
            "storage", "blob", "upload",
            "--container-name", $Container,
            "--name", $TaskBundleBlobName,
            "--file", $bundleZip,
            "--overwrite", "true",
            "--output", "none"
        ) | Out-Null
    } finally {
        if ($null -eq $previousStorageConnectionString) {
            Remove-Item Env:AZURE_STORAGE_CONNECTION_STRING -ErrorAction SilentlyContinue
        } else {
            $env:AZURE_STORAGE_CONNECTION_STRING = $previousStorageConnectionString
        }
        Remove-Item -LiteralPath $bundleZip -Force -ErrorAction SilentlyContinue
    }
    Write-Host "  [OK]" -ForegroundColor Green
}

function Get-PoolUri {
    return "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Batch/batchAccounts/$BatchAccount/pools/${BatchPoolId}?api-version=2025-06-01"
}

function New-PoolDefinition {
    return @{
        identity = @{
            type = "UserAssigned"
            userAssignedIdentities = @{
                $ManagedIdentityId = @{}
            }
        }
        properties = @{
            vmSize = $VmSize
            deploymentConfiguration = @{
                virtualMachineConfiguration = @{
                    imageReference = @{
                        publisher = $PoolImagePublisher
                        offer = $PoolImageOffer
                        sku = $PoolImageSku
                        version = $PoolImageVersion
                    }
                    nodeAgentSkuId = $PoolNodeAgentSkuId
                }
            }
            scaleSettings = @{
                autoScale = @{
                    evaluationInterval = "PT5M"
                    formula = $PoolAutoScaleFormula
                }
            }
            taskSchedulingPolicy = @{
                nodeFillType = "Spread"
            }
            taskSlotsPerNode = 1
        }
    }
}

function Get-CurrentPoolDefinition {
    $response = Invoke-Az -Args @("rest", "--method", "get", "--uri", (Get-PoolUri), "--output", "json") -AllowFailure
    if (-not $response.Success -or [string]::IsNullOrWhiteSpace($response.Output)) {
        return $null
    }
    return ($response.Output | ConvertFrom-Json)
}

function Test-PoolMatchesDesired([object]$CurrentPool, [object]$DesiredPool) {
    if ($null -eq $CurrentPool) {
        return $false
    }

    if ([string]$CurrentPool.properties.vmSize -ine [string]$DesiredPool.properties.vmSize) {
        return $false
    }

    $currentVmConfig = $CurrentPool.properties.deploymentConfiguration.virtualMachineConfiguration
    $desiredVmConfig = $DesiredPool.properties.deploymentConfiguration.virtualMachineConfiguration
    foreach ($field in @("publisher", "offer", "sku", "version")) {
        if ([string]$currentVmConfig.imageReference.$field -ne [string]$desiredVmConfig.imageReference.$field) {
            return $false
        }
    }
    if ([string]$currentVmConfig.nodeAgentSkuId -ne [string]$desiredVmConfig.nodeAgentSkuId) {
        return $false
    }
    $hasContainerConfiguration = ($currentVmConfig.PSObject.Properties.Name -contains "containerConfiguration") -and ($null -ne $currentVmConfig.containerConfiguration)
    if ($hasContainerConfiguration) {
        return $false
    }

    $identityIds = @()
    $hasUserAssignedIdentities = ($CurrentPool.identity.PSObject.Properties.Name -contains "userAssignedIdentities") -and ($null -ne $CurrentPool.identity.userAssignedIdentities)
    if ($hasUserAssignedIdentities) {
        $identityIds = @($CurrentPool.identity.userAssignedIdentities.PSObject.Properties.Name)
    }
    if ($identityIds.Count -ne 1 -or $identityIds[0] -ne $ManagedIdentityId) {
        return $false
    }

    $currentAutoScale = $CurrentPool.properties.scaleSettings.autoScale
    $desiredAutoScale = $DesiredPool.properties.scaleSettings.autoScale
    if ([string]$currentAutoScale.evaluationInterval -ne [string]$desiredAutoScale.evaluationInterval) {
        return $false
    }
    if ((Normalize-Whitespace ([string]$currentAutoScale.formula)) -ne (Normalize-Whitespace ([string]$desiredAutoScale.formula))) {
        return $false
    }

    return $true
}

function Ensure-BatchPool {
    $desiredPool = New-PoolDefinition
    $currentPool = Get-CurrentPoolDefinition
    $needsRefresh = $RefreshBatchPool.IsPresent -or -not (Test-PoolMatchesDesired $currentPool $desiredPool)

    if (-not $needsRefresh) {
        Write-Status "Batch pool host runtime already matches desired state" "SKIPPED" DarkGray
        return
    }

    Write-Host "  Ensuring Batch pool host runtime..." -NoNewline
    $poolUri = Get-PoolUri
    $poolFile = [System.IO.Path]::GetTempFileName() + ".json"
    $desiredPool | ConvertTo-Json -Depth 30 | Set-Content $poolFile -Encoding UTF8

    try {
        if ($null -ne $currentPool) {
            Invoke-Az -Args @("rest", "--method", "delete", "--uri", $poolUri, "--output", "none") -AllowFailure | Out-Null
            for ($attempt = 0; $attempt -lt 36; $attempt++) {
                $probe = Invoke-Az -Args @("rest", "--method", "get", "--uri", $poolUri, "--output", "none") -AllowFailure
                if (-not $probe.Success) {
                    break
                }
                Start-Sleep -Seconds 5
            }
        }

        Invoke-Az -Args @(
            "rest", "--method", "put",
            "--uri", $poolUri,
            "--body", "@$poolFile",
            "--output", "none"
        ) | Out-Null
    } finally {
        Remove-Item $poolFile -Force -ErrorAction SilentlyContinue
    }

    Write-Host "  [OK]" -ForegroundColor Green
}

function Convert-ToAdfLiteral([string]$Value) {
    return $Value.Replace("'", "''")
}

function New-AdfHostCommandExpression([string]$ScriptPath) {
    $bootstrap = "set -euo pipefail; python3 -m pip --version >/dev/null 2>&1 || python3 -m ensurepip --upgrade; python3 -m pip install --user uv >/dev/null; export PATH=""`$HOME/.local/bin:`$PATH""; python3 -c ""import zipfile; zipfile.ZipFile('sec-edgar-task.zip').extractall('app')""; cd app; uv sync --no-dev; SEC_USER_AGENT=""$SecUserAgent"" CLOUD_PROVIDER=azure AZURE_STORAGE_ACCOUNT=$StorageAccount AZURE_CONTAINER=$Container STORAGE_PREFIX=$Prefix AZURE_CLIENT_ID=$ManagedIdentityClientId .venv/bin/python $ScriptPath --date "
    $prefix = "/bin/bash -lc '$bootstrap"
    $suffix = "'"
    return "@concat('" + (Convert-ToAdfLiteral $prefix) + "', pipeline().parameters.ingestDate, '" + (Convert-ToAdfLiteral $suffix) + "')"
}

function Get-DesiredLinkedServices {
    $definition = Get-Content $LinkedServicesPath -Raw | ConvertFrom-Json
    foreach ($linkedService in $definition.linkedServices) {
        if ($linkedService.name -eq "AzureStorageLS") {
            $linkedService.properties.typeProperties.connectionString = [pscustomobject]@{
                type = "SecureString"
                value = $StorageConnectionString
            }
        }
        if ($linkedService.name -eq "AzureBatchLS") {
            $linkedService.properties.typeProperties.accountName = $BatchAccount
            $linkedService.properties.typeProperties.accessKey = [pscustomobject]@{
                type = "SecureString"
                value = $BatchAccessKey
            }
            $linkedService.properties.typeProperties.batchUri = $BatchUri
            $linkedService.properties.typeProperties.poolName = $BatchPoolId
        }
    }
    return $definition.linkedServices
}

function Get-DesiredPipelineProperties {
    $definition = Get-Content $PipelinePath -Raw | ConvertFrom-Json
    $commandScripts = @{
        "IngestTickersExchange" = "scripts/ingest/01_ingest_tickers_exchange.py"
        "IngestSubmissions" = "scripts/ingest/02_ingest_submissions.py"
        "IngestCompanyFacts" = "scripts/ingest/03_ingest_companyfacts.py"
    }
    foreach ($activity in $definition.properties.activities) {
        if ($commandScripts.ContainsKey($activity.name)) {
            $activity.typeProperties.resourceLinkedService = [pscustomobject]@{
                referenceName = "AzureStorageLS"
                type = "LinkedServiceReference"
            }
            $activity.typeProperties.folderPath = $TaskBundleFolderPath
            $activity.typeProperties.referenceObjects = [pscustomobject]@{
                linkedServices = @()
                datasets = @()
            }
            $activity.typeProperties.command = [pscustomobject]@{
                type = "Expression"
                value = New-AdfHostCommandExpression $commandScripts[$activity.name]
            }
        }
    }
    return $definition.properties
}

function Get-DesiredTriggerProperties {
    $definition = Get-Content $TriggerPath -Raw | ConvertFrom-Json
    $definition.properties.pipeline.pipelineReference.referenceName = $PipelineName
    return $definition.properties
}

function Get-LivePipelineProperties {
    $response = Invoke-Az -Args @(
        "datafactory", "pipeline", "show",
        "--factory-name", $AdfName,
        "--resource-group", $ResourceGroup,
        "--name", $PipelineName,
        "--output", "json"
    ) -AllowFailure
    if (-not $response.Success -or [string]::IsNullOrWhiteSpace($response.Output)) {
        return $null
    }
    return (($response.Output | ConvertFrom-Json).properties)
}

function Get-LiveTrigger {
    $response = Invoke-Az -Args @(
        "datafactory", "trigger", "show",
        "--factory-name", $AdfName,
        "--resource-group", $ResourceGroup,
        "--name", $TriggerName,
        "--output", "json"
    ) -AllowFailure
    if (-not $response.Success -or [string]::IsNullOrWhiteSpace($response.Output)) {
        return $null
    }
    return ($response.Output | ConvertFrom-Json)
}

function Get-LiveTriggerProperties {
    $liveTrigger = Get-LiveTrigger
    if ($null -eq $liveTrigger) {
        return $null
    }

    $properties = $liveTrigger.properties
    if ($properties.PSObject.Properties.Name -contains "runtimeState") {
        $null = $properties.PSObject.Properties.Remove("runtimeState")
    }
    return $properties
}

function Invoke-AdfCreate([string]$Kind, [string]$Name, [string]$ArgumentName, [object]$Properties) {
    $tmpFile = [System.IO.Path]::GetTempFileName() + ".json"
    try {
        $Properties | ConvertTo-Json -Depth 30 | Set-Content $tmpFile -Encoding UTF8
        Invoke-Az -Args @(
            "datafactory", $Kind, "create",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $Name,
            $ArgumentName, "@$tmpFile",
            "--output", "none"
        ) | Out-Null
    } finally {
        Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    }
}

function Stop-TriggerIfStarted {
    $liveTrigger = Get-LiveTrigger
    if ($null -eq $liveTrigger) {
        return
    }
    if ([string]$liveTrigger.properties.runtimeState -eq "Started") {
        Invoke-Az -Args @(
            "datafactory", "trigger", "stop",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $TriggerName,
            "--output", "none"
        ) | Out-Null
    }
}

function Reinstall-AdfObjectsIfRequested {
    if (-not $ReinstallAdfObjects.IsPresent) {
        return
    }

    $liveTrigger = Get-LiveTrigger
    if ($null -ne $liveTrigger) {
        Stop-TriggerIfStarted
        Invoke-Az -Args @(
            "datafactory", "trigger", "delete",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $TriggerName,
            "--yes",
            "--output", "none"
        ) | Out-Null
        Invoke-Az -Args @(
            "datafactory", "trigger", "wait",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $TriggerName,
            "--deleted",
            "--interval", "5",
            "--timeout", "120",
            "--output", "none"
        ) | Out-Null
    }

    $livePipeline = Get-LivePipelineProperties
    if ($null -ne $livePipeline) {
        Invoke-Az -Args @(
            "datafactory", "pipeline", "delete",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $PipelineName,
            "--yes",
            "--output", "none"
        ) | Out-Null
    }
}

function Ensure-LinkedServices {
    foreach ($linkedService in (Get-DesiredLinkedServices)) {
        Write-Host "  Linked service: $($linkedService.name)" -NoNewline
        Invoke-AdfCreate "linked-service" $linkedService.name "--properties" $linkedService.properties
        Write-Host "  [OK]" -ForegroundColor Green
    }
}

function Ensure-Pipeline {
    $desiredProperties = Get-DesiredPipelineProperties
    Write-Host "  Pipeline: $PipelineName" -NoNewline
    Invoke-AdfCreate "pipeline" $PipelineName "--pipeline" $desiredProperties
    Write-Host "  [OK]" -ForegroundColor Green
}

function Ensure-Trigger {
    $desiredProperties = Get-DesiredTriggerProperties
    Stop-TriggerIfStarted
    Write-Host "  Trigger: $TriggerName" -NoNewline
    Invoke-AdfCreate "trigger" $TriggerName "--properties" $desiredProperties
    Write-Host "  [OK]" -ForegroundColor Green

    $liveTrigger = Get-LiveTrigger
    if ($null -eq $liveTrigger -or [string]$liveTrigger.properties.runtimeState -ne "Started") {
        Invoke-Az -Args @(
            "datafactory", "trigger", "start",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $TriggerName,
            "--output", "none"
        ) | Out-Null
        Write-Status "Trigger active - fires daily at 06:00 UTC" "OK" Green
    } else {
        Write-Status "Trigger already started" "SKIPPED" DarkGray
    }
}

function Ensure-AdfArtifacts {
    Write-Section "[4/4] Ensuring ADF linked services, pipeline, and trigger..."
    Reinstall-AdfObjectsIfRequested
    Ensure-LinkedServices
    Ensure-Pipeline
    Ensure-Trigger
}

function Write-Summary {
    $today = Get-Date -Format "yyyy-MM-dd"
    Write-Host ""
    Write-Host "=================================================================" -ForegroundColor Green
    Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
    Write-Host "=================================================================" -ForegroundColor Green
    Write-Host "  Pipeline        : $PipelineName"
    Write-Host "  Trigger         : $TriggerName"
    Write-Host "  Task bundle     : $Container/$TaskBundleBlobName"
    Write-Host "  Runtime         : Azure Batch host VM (non-container pool)"
    if ($BuildLegacyDockerArtifact.IsPresent) {
        Write-Host "  Legacy image    : $AcrName.azurecr.io/$LegacyImageName"
    } else {
        Write-Host "  Legacy image    : skipped (enable with -BuildLegacyDockerArtifact)"
    }
    Write-Host "  Refresh pool    : $($RefreshBatchPool.IsPresent)"
    Write-Host "  Reinstall ADF   : $($ReinstallAdfObjects.IsPresent)"
    Write-Host ""
    Write-Host "  To run immediately:"
    Write-Host "  az datafactory pipeline create-run ``"
    Write-Host "    --factory-name $AdfName ``"
    Write-Host "    --resource-group $ResourceGroup ``"
    Write-Host "    --name $PipelineName ``"
    Write-Host "    --parameters '{""ingestDate"":""$today""}'"
    Write-Host "=================================================================" -ForegroundColor Green
}

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  SEC EDGAR Bronze Layer - Azure Deployment" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  Resource Group   = $ResourceGroup"
Write-Host "  Storage Account  = $StorageAccount (container=$Container)"
Write-Host "  Batch Account    = $BatchAccount"
Write-Host "  ADF              = $AdfName"
Write-Host "  Managed Identity = $ManagedIdentity"
Write-Host "  Azure Config Dir = $AzConfigDir"
Write-Host "=================================================================" -ForegroundColor Cyan

Ensure-AzureContext
Get-SecretMaterial
Ensure-Rbac

Write-Section "[3/4] Staging runtime artifacts..."
Publish-LegacyDockerArtifact
Publish-TaskBundle
Ensure-BatchPool

Ensure-AdfArtifacts
Write-Summary
