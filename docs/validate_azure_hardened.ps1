# validate_azure_hardened.ps1 - SEC EDGAR platform Azure validation

[CmdletBinding()]
param(
    [string]$StorageAccount = $(if ($env:AZURE_STORAGE_ACCOUNT) { $env:AZURE_STORAGE_ACCOUNT } else { "mysecedgarstorage" }),
    [string]$Container = $(if ($env:AZURE_CONTAINER) { $env:AZURE_CONTAINER } else { "sec-edgar" }),
    [string]$Prefix = $(if ($env:STORAGE_PREFIX) { $env:STORAGE_PREFIX } else { "sec-edgar" }),
    [string]$BatchAccount = $(if ($env:AZURE_BATCH_ACCOUNT) { $env:AZURE_BATCH_ACCOUNT } else { "mysecedgarbatch" }),
    [string]$BatchPoolId = $(if ($env:AZURE_BATCH_POOL_ID) { $env:AZURE_BATCH_POOL_ID } else { "sec-edgar-pool" }),
    [string]$AcrName = $(if ($env:AZURE_ACR_NAME) { $env:AZURE_ACR_NAME } else { "mysecedgaracr" }),
    [string]$AdfName = $(if ($env:AZURE_DATA_FACTORY_NAME) { $env:AZURE_DATA_FACTORY_NAME } else { "mysecedgaradf" }),
    [string]$ResourceGroup = $(if ($env:AZURE_RESOURCE_GROUP) { $env:AZURE_RESOURCE_GROUP } else { "my-sec-edgar-rg" }),
    [string]$ManagedIdentity = $(if ($env:AZURE_MANAGED_IDENTITY_NAME) { $env:AZURE_MANAGED_IDENTITY_NAME } else { "sec-edgar-ingest-identity" }),
    [string]$PipelineName = $(if ($env:ADF_PIPELINE_NAME) { $env:ADF_PIPELINE_NAME } else { "sec-edgar-bronze-ingest" }),
    [string]$TriggerName = $(if ($env:ADF_TRIGGER_NAME) { $env:ADF_TRIGGER_NAME } else { "DailyBronzeIngestTrigger" })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Pass = 0
$Fail = 0
$Warn = 0
$AzureCliExe = $null
$AzureCliResolved = $false
$UsePythonAzCli = $false

function Write-Pass([string]$Message) { Write-Host "  PASS  $Message" -ForegroundColor Green; $script:Pass++ }
function Write-Fail([string]$Message) { Write-Host "  FAIL  $Message" -ForegroundColor Red; $script:Fail++ }
function Write-Warn([string]$Message) { Write-Host "  WARN  $Message" -ForegroundColor Yellow; $script:Warn++ }
function Write-Hdr([string]$Message) { Write-Host ""; Write-Host "-- $Message --" -ForegroundColor Cyan }

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
    param([Parameter(Mandatory = $true)][string[]]$Args)

    Resolve-AzRunner
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        if ($script:UsePythonAzCli) {
            $output = & $script:AzureCliExe -m azure.cli @Args --only-show-errors 2>&1
        } else {
            $output = & $script:AzureCliExe @Args --only-show-errors 2>&1
        }
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $exitCode = $LASTEXITCODE
    $text = ($output | Out-String).Trim()

    return [pscustomobject]@{
        Success = ($exitCode -eq 0)
        Output = $text
    }
}

function Get-AzJsonOrNull {
    param([Parameter(Mandatory = $true)][string[]]$Args)

    $result = Invoke-Az -Args $Args
    if (-not $result.Success -or [string]::IsNullOrWhiteSpace($result.Output)) {
        return $null
    }
    return ($result.Output | ConvertFrom-Json)
}

function Get-AzTextOrNull {
    param([Parameter(Mandatory = $true)][string[]]$Args)

    $result = Invoke-Az -Args $Args
    if (-not $result.Success) {
        return $null
    }
    return $result.Output
}

function Test-ReferenceName($Reference, [string]$ExpectedName) {
    return ($null -ne $Reference -and [string]$Reference.referenceName -eq $ExpectedName)
}

Write-Hdr "0. Login"
$account = Get-AzJsonOrNull @("account", "show", "--output", "json")
if ($null -eq $account) {
    Write-Host "Not logged in. Run: az login" -ForegroundColor Red
    exit 1
}
$subscription = [string]$account.id
Write-Pass "Logged in - subscription: $($account.name) ($subscription)"

Write-Hdr "1. Resource Group"
$rg = Get-AzJsonOrNull @("group", "show", "--name", $ResourceGroup, "--output", "json")
if ($null -eq $rg) {
    Write-Fail "Resource group '$ResourceGroup' not found"
} else {
    Write-Pass "Resource group '$ResourceGroup' exists (location: $($rg.location))"
}

Write-Hdr "2. Storage"
$storage = Get-AzJsonOrNull @("storage", "account", "show", "--name", $StorageAccount, "--resource-group", $ResourceGroup, "--output", "json")
if ($null -eq $storage) {
    Write-Fail "Storage account '$StorageAccount' not found in '$ResourceGroup'"
} else {
    Write-Pass "Storage account '$StorageAccount' exists (SKU: $($storage.sku.name))"
    if ($storage.isHnsEnabled -eq $true) { Write-Pass "Hierarchical Namespace enabled (ADLS Gen2)" } else { Write-Fail "Hierarchical Namespace is not enabled" }
    if ($storage.minimumTlsVersion -eq "TLS1_2") { Write-Pass "Minimum TLS 1.2 enforced" } else { Write-Warn "Minimum TLS is '$($storage.minimumTlsVersion)'" }
    if ($storage.allowBlobPublicAccess -eq $false) { Write-Pass "Public blob access blocked" } else { Write-Warn "Public blob access is not blocked" }
}

$filesystem = Get-AzJsonOrNull @("storage", "fs", "show", "--name", $Container, "--account-name", $StorageAccount, "--auth-mode", "login", "--output", "json")
if ($null -eq $filesystem) {
    Write-Fail "Container '$Container' not found"
} else {
    Write-Pass "Container '$Container' exists"
}

$taskBundle = Get-AzJsonOrNull @(
    "storage", "fs", "file", "show",
    "--file-system", $Container,
    "--path", "adf-resources/sec-edgar-task.zip",
    "--account-name", $StorageAccount,
    "--auth-mode", "login",
    "--output", "json"
)
if ($null -ne $taskBundle) {
    Write-Pass "ADF task bundle exists at '$Container/adf-resources/sec-edgar-task.zip'"
} else {
    Write-Warn "ADF task bundle missing at '$Container/adf-resources/sec-edgar-task.zip'"
}

Write-Hdr "3. Batch"
$batch = Get-AzJsonOrNull @("batch", "account", "show", "--name", $BatchAccount, "--resource-group", $ResourceGroup, "--output", "json")
if ($null -eq $batch) {
    Write-Fail "Batch account '$BatchAccount' not found"
} else {
    Write-Pass "Batch account '$BatchAccount' exists"
    $loginResult = Invoke-Az -Args @("batch", "account", "login", "--name", $BatchAccount, "--resource-group", $ResourceGroup, "--output", "none")
    if (-not $loginResult.Success) {
        Write-Fail "Batch account login failed for '$BatchAccount'"
    } else {
        $pool = Get-AzJsonOrNull @("batch", "pool", "show", "--pool-id", $BatchPoolId, "--account-name", $BatchAccount, "--output", "json")
        if ($null -eq $pool) {
            Write-Fail "Batch pool '$BatchPoolId' not found"
        } else {
            Write-Pass "Batch pool '$BatchPoolId' exists (VM: $($pool.vmSize))"
            if ([string]$pool.vmSize -ieq "STANDARD_D2S_V3") { Write-Pass "VM size is Standard_D2s_v3" } else { Write-Warn "VM size is '$($pool.vmSize)'" }
            if ($pool.enableAutoScale -eq $true) { Write-Pass "Auto-scale enabled" } else { Write-Warn "Auto-scale is not enabled" }
            if ([int]$pool.targetLowPriorityNodes -eq 0) { Write-Pass "Low-priority nodes disabled" } else { Write-Warn "Low-priority target nodes = $($pool.targetLowPriorityNodes)" }
            $vmConfig = if ($pool.PSObject.Properties.Name -contains "deploymentConfiguration") { $pool.deploymentConfiguration.virtualMachineConfiguration } else { $pool.virtualMachineConfiguration }
            if ($vmConfig.imageReference.publisher -eq "microsoft-dsvm" -and $vmConfig.imageReference.offer -eq "ubuntu-hpc" -and $vmConfig.imageReference.sku -eq "2204") {
                Write-Pass "Pool image is microsoft-dsvm/ubuntu-hpc/2204"
            } else {
                Write-Warn "Pool image is '$($vmConfig.imageReference.publisher)/$($vmConfig.imageReference.offer)/$($vmConfig.imageReference.sku)'"
            }
            $hasContainerConfiguration = ($vmConfig.PSObject.Properties.Name -contains "containerConfiguration") -and ($null -ne $vmConfig.containerConfiguration)
            if (-not $hasContainerConfiguration) { Write-Pass "Pool has no containerConfiguration" } else { Write-Fail "Pool still has containerConfiguration" }
        }
    }
}

Write-Hdr "4. Data Factory"
$adf = Get-AzJsonOrNull @("datafactory", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--output", "json")
if ($null -eq $adf) {
    Write-Fail "ADF '$AdfName' not found"
} else {
    Write-Pass "ADF '$AdfName' exists"
    if ([string]$adf.identity.type -eq "SystemAssigned") { Write-Pass "ADF has a system-assigned managed identity" } else { Write-Warn "ADF identity type is '$($adf.identity.type)'" }

    $storageLs = Get-AzJsonOrNull @("datafactory", "linked-service", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--linked-service-name", "AzureStorageLS", "--output", "json")
    if ($null -eq $storageLs) {
        Write-Fail "Linked service 'AzureStorageLS' not found"
    } else {
        $storageLsProperties = if ($storageLs.PSObject.Properties.Name -contains "properties") { $storageLs.properties } else { $storageLs }
        if ([string]$storageLsProperties.type -eq "AzureBlobStorage") { Write-Pass "AzureStorageLS type is AzureBlobStorage" } else { Write-Fail "AzureStorageLS type is '$($storageLsProperties.type)'" }
        if ($storageLsProperties.PSObject.Properties.Name -contains "connectionString") { Write-Pass "AzureStorageLS uses connectionString auth" } else { Write-Fail "AzureStorageLS is missing connectionString auth" }
    }

    $batchLs = Get-AzJsonOrNull @("datafactory", "linked-service", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--linked-service-name", "AzureBatchLS", "--output", "json")
    if ($null -eq $batchLs) {
        Write-Fail "Linked service 'AzureBatchLS' not found"
    } else {
        $batchLsProperties = if ($batchLs.PSObject.Properties.Name -contains "properties") { $batchLs.properties } else { $batchLs }
        if ([string]$batchLsProperties.type -eq "AzureBatch") { Write-Pass "AzureBatchLS type is AzureBatch" } else { Write-Fail "AzureBatchLS type is '$($batchLsProperties.type)'" }
        if ($batchLsProperties.PSObject.Properties.Name -contains "accessKey") { Write-Pass "AzureBatchLS uses accessKey auth" } else { Write-Fail "AzureBatchLS is missing accessKey auth" }
        if ([string]$batchLsProperties.accountName -eq $BatchAccount) { Write-Pass "AzureBatchLS accountName matches '$BatchAccount'" } else { Write-Warn "AzureBatchLS accountName is '$($batchLsProperties.accountName)'" }
        if ([string]$batchLsProperties.poolName -eq $BatchPoolId) { Write-Pass "AzureBatchLS poolName matches '$BatchPoolId'" } else { Write-Warn "AzureBatchLS poolName is '$($batchLsProperties.poolName)'" }
    }

    $pipeline = Get-AzJsonOrNull @("datafactory", "pipeline", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--name", $PipelineName, "--output", "json")
    if ($null -eq $pipeline) {
        Write-Fail "Pipeline '$PipelineName' not found"
    } else {
        Write-Pass "Pipeline '$PipelineName' exists"
        $pipelineProperties = if ($pipeline.PSObject.Properties.Name -contains "properties") { $pipeline.properties } else { $pipeline }
        foreach ($activityName in @("IngestTickersExchange", "IngestSubmissions", "IngestCompanyFacts")) {
            $activity = $pipelineProperties.activities | Where-Object { $_.name -eq $activityName } | Select-Object -First 1
            if ($null -eq $activity) {
                Write-Fail "Pipeline activity '$activityName' is missing"
                continue
            }
            $activityProperties = if ($activity.PSObject.Properties.Name -contains "typeProperties") { $activity.typeProperties } else { $activity }
            if (Test-ReferenceName $activity.linkedServiceName "AzureBatchLS") { Write-Pass "$activityName uses AzureBatchLS" } else { Write-Fail "$activityName does not reference AzureBatchLS" }
            if (Test-ReferenceName $activityProperties.resourceLinkedService "AzureStorageLS") { Write-Pass "$activityName stages resources from AzureStorageLS" } else { Write-Fail "$activityName is missing AzureStorageLS staging" }
            if ([string]$activityProperties.folderPath -eq "$Container/adf-resources") { Write-Pass "$activityName folderPath is '$Container/adf-resources'" } else { Write-Fail "$activityName folderPath is '$($activityProperties.folderPath)'" }
        }
    }

    $trigger = Get-AzJsonOrNull @("datafactory", "trigger", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--name", $TriggerName, "--output", "json")
    if ($null -eq $trigger) {
        Write-Fail "Trigger '$TriggerName' not found"
    } else {
        Write-Pass "Trigger '$TriggerName' exists"
        if ([string]$trigger.properties.runtimeState -eq "Started") { Write-Pass "Trigger '$TriggerName' is started" } else { Write-Warn "Trigger '$TriggerName' runtimeState is '$($trigger.properties.runtimeState)'" }
        if ([string]$trigger.properties.pipeline.pipelineReference.referenceName -eq $PipelineName) { Write-Pass "Trigger targets pipeline '$PipelineName'" } else { Write-Fail "Trigger points to '$($trigger.properties.pipeline.pipelineReference.referenceName)'" }
    }
}

Write-Hdr "5. Managed Identity RBAC"
$identity = Get-AzJsonOrNull @("identity", "show", "--name", $ManagedIdentity, "--resource-group", $ResourceGroup, "--output", "json")
if ($null -eq $identity) {
    Write-Fail "Managed identity '$ManagedIdentity' not found"
} else {
    Write-Pass "Managed identity '$ManagedIdentity' found"
    $storageScope = "/subscriptions/$subscription/resourceGroups/$ResourceGroup/providers/Microsoft.Storage/storageAccounts/$StorageAccount"
    $storageRbacEntries = Get-AzJsonOrNull @(
        "role", "assignment", "list",
        "--assignee-object-id", [string]$identity.principalId,
        "--role", "Storage Blob Data Contributor",
        "--scope", $storageScope,
        "--output", "json"
    )
    if (@($storageRbacEntries).Count -ge 1) {
        Write-Pass "Storage Blob Data Contributor assigned on the storage account"
    } else {
        Write-Fail "Storage Blob Data Contributor is missing on the storage account"
    }
    Write-Warn "AcrPull is intentionally not required for the host-executed runtime"
}

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  PASS: $Pass   FAIL: $Fail   WARN: $Warn" -ForegroundColor White
Write-Host "======================================" -ForegroundColor Cyan

if ($Fail -gt 0) { exit 1 } else { exit 0 }
