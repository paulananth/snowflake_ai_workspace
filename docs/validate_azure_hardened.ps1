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
    [string]$TriggerName = $(if ($env:ADF_TRIGGER_NAME) { $env:ADF_TRIGGER_NAME } else { "DailyBronzeIngestTrigger" }),
    [string]$MonthlyTriggerName = $(if ($env:ADF_MONTHLY_TRIGGER_NAME) { $env:ADF_MONTHLY_TRIGGER_NAME } else { "MonthlyBronzeFullRefreshTrigger" }),
    [string]$FunctionAppName = $(if ($env:AZURE_FUNCTION_APP_NAME) { $env:AZURE_FUNCTION_APP_NAME } else { "" }),
    [string]$FunctionLinkedServiceName = $(if ($env:ADF_FUNCTION_LINKED_SERVICE_NAME) { $env:ADF_FUNCTION_LINKED_SERVICE_NAME } else { "AzureFunctionBronzeLS" })
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

function Get-LsProperties($LinkedService) {
    if ($LinkedService.PSObject.Properties.Name -contains "properties") {
        return $LinkedService.properties
    }
    return $LinkedService
}

Write-Hdr "0. Login"
$account = Get-AzJsonOrNull @("account", "show", "--output", "json")
if ($null -eq $account) {
    Write-Host "Not logged in. Run: az login" -ForegroundColor Red
    exit 1
}
$subscription = [string]$account.id
$suffix = $subscription.Replace("-", "").Substring(0, 8).ToLowerInvariant()
if ([string]::IsNullOrWhiteSpace($FunctionAppName)) {
    $FunctionAppName = "sec-edgar-flex-$suffix"
}
Write-Pass "Logged in - subscription: $($account.name) ($subscription)"
Write-Pass "Derived Function App name: $FunctionAppName"

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

Write-Hdr "4. Function App"
$functionApp = Get-AzJsonOrNull @("functionapp", "show", "--name", $FunctionAppName, "--resource-group", $ResourceGroup, "--output", "json")
if ($null -eq $functionApp) {
    Write-Fail "Function App '$FunctionAppName' not found"
} else {
    Write-Pass "Function App '$FunctionAppName' exists"
    if ([string]$functionApp.kind -match "functionapp") { Write-Pass "Function App kind is '$($functionApp.kind)'" } else { Write-Warn "Unexpected Function App kind '$($functionApp.kind)'" }
    if ($null -ne $functionApp.identity -and -not [string]::IsNullOrWhiteSpace([string]$functionApp.identity.principalId)) { Write-Pass "Function App has a managed identity" } else { Write-Fail "Function App is missing a managed identity" }

    $functions = Get-AzJsonOrNull @("functionapp", "function", "list", "--name", $FunctionAppName, "--resource-group", $ResourceGroup, "--output", "json")
    if ($null -eq $functions) {
        Write-Fail "Could not list functions for '$FunctionAppName'"
    } else {
        $hasTickers = @($functions | Where-Object { [string]$_.name -like "*ingest_tickers_exchange" }).Count -gt 0
        $hasDailyIndex = @($functions | Where-Object { [string]$_.name -like "*ingest_daily_index" }).Count -gt 0
        if ($hasTickers) { Write-Pass "Function 'ingest_tickers_exchange' is indexed" } else { Write-Fail "Function 'ingest_tickers_exchange' is missing" }
        if ($hasDailyIndex) { Write-Pass "Function 'ingest_daily_index' is indexed" } else { Write-Fail "Function 'ingest_daily_index' is missing" }
    }
}

Write-Hdr "5. Data Factory"
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
        $storageLsProperties = Get-LsProperties $storageLs
        if ([string]$storageLsProperties.type -eq "AzureBlobStorage") { Write-Pass "AzureStorageLS type is AzureBlobStorage" } else { Write-Fail "AzureStorageLS type is '$($storageLsProperties.type)'" }
        if ($storageLsProperties.PSObject.Properties.Name -contains "connectionString") { Write-Pass "AzureStorageLS uses connectionString auth" } else { Write-Fail "AzureStorageLS is missing connectionString auth" }
    }

    $batchLs = Get-AzJsonOrNull @("datafactory", "linked-service", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--linked-service-name", "AzureBatchLS", "--output", "json")
    if ($null -eq $batchLs) {
        Write-Fail "Linked service 'AzureBatchLS' not found"
    } else {
        $batchLsProperties = Get-LsProperties $batchLs
        if ([string]$batchLsProperties.type -eq "AzureBatch") { Write-Pass "AzureBatchLS type is AzureBatch" } else { Write-Fail "AzureBatchLS type is '$($batchLsProperties.type)'" }
        if ($batchLsProperties.PSObject.Properties.Name -contains "accessKey") { Write-Pass "AzureBatchLS uses accessKey auth" } else { Write-Fail "AzureBatchLS is missing accessKey auth" }
        if ([string]$batchLsProperties.accountName -eq $BatchAccount) { Write-Pass "AzureBatchLS accountName matches '$BatchAccount'" } else { Write-Warn "AzureBatchLS accountName is '$($batchLsProperties.accountName)'" }
        if ([string]$batchLsProperties.poolName -eq $BatchPoolId) { Write-Pass "AzureBatchLS poolName matches '$BatchPoolId'" } else { Write-Warn "AzureBatchLS poolName is '$($batchLsProperties.poolName)'" }
    }

    $functionLs = Get-AzJsonOrNull @("datafactory", "linked-service", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--linked-service-name", $FunctionLinkedServiceName, "--output", "json")
    if ($null -eq $functionLs) {
        Write-Fail "Linked service '$FunctionLinkedServiceName' not found"
    } else {
        $functionLsProperties = Get-LsProperties $functionLs
        if ([string]$functionLsProperties.type -eq "AzureFunction") { Write-Pass "$FunctionLinkedServiceName type is AzureFunction" } else { Write-Fail "$FunctionLinkedServiceName type is '$($functionLsProperties.type)'" }
        if ($functionLsProperties.PSObject.Properties.Name -contains "functionAppUrl") { Write-Pass "$FunctionLinkedServiceName has functionAppUrl" } else { Write-Fail "$FunctionLinkedServiceName is missing functionAppUrl" }
        if ($functionLsProperties.PSObject.Properties.Name -contains "functionKey") { Write-Pass "$FunctionLinkedServiceName has functionKey auth" } else { Write-Fail "$FunctionLinkedServiceName is missing functionKey auth" }
    }

    $pipeline = Get-AzJsonOrNull @("datafactory", "pipeline", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--name", $PipelineName, "--output", "json")
    if ($null -eq $pipeline) {
        Write-Fail "Pipeline '$PipelineName' not found"
    } else {
        Write-Pass "Pipeline '$PipelineName' exists"
        $pipelineProperties = if ($pipeline.PSObject.Properties.Name -contains "properties") { $pipeline.properties } else { $pipeline }
        if ($pipelineProperties.parameters.PSObject.Properties.Name -contains "ingestDate") { Write-Pass "Pipeline parameter 'ingestDate' exists" } else { Write-Fail "Pipeline parameter 'ingestDate' is missing" }
        if ($pipelineProperties.parameters.PSObject.Properties.Name -contains "fullRefresh") { Write-Pass "Pipeline parameter 'fullRefresh' exists" } else { Write-Fail "Pipeline parameter 'fullRefresh' is missing" }

        $tickers = $pipelineProperties.activities | Where-Object { $_.name -eq "IngestTickersExchange" } | Select-Object -First 1
        if ($null -eq $tickers) {
            Write-Fail "Pipeline activity 'IngestTickersExchange' is missing"
        } else {
            if ([string]$tickers.type -eq "AzureFunctionActivity") { Write-Pass "IngestTickersExchange uses AzureFunctionActivity" } else { Write-Fail "IngestTickersExchange type is '$($tickers.type)'" }
            if (Test-ReferenceName $tickers.linkedServiceName $FunctionLinkedServiceName) { Write-Pass "IngestTickersExchange uses $FunctionLinkedServiceName" } else { Write-Fail "IngestTickersExchange does not reference $FunctionLinkedServiceName" }
            if ([string]$tickers.typeProperties.functionName -eq "ingest_tickers_exchange") { Write-Pass "IngestTickersExchange functionName is ingest_tickers_exchange" } else { Write-Fail "IngestTickersExchange functionName is '$($tickers.typeProperties.functionName)'" }
        }

        $dailyIndex = $pipelineProperties.activities | Where-Object { $_.name -eq "IngestDailyIndex" } | Select-Object -First 1
        if ($null -eq $dailyIndex) {
            Write-Fail "Pipeline activity 'IngestDailyIndex' is missing"
        } else {
            if ([string]$dailyIndex.type -eq "AzureFunctionActivity") { Write-Pass "IngestDailyIndex uses AzureFunctionActivity" } else { Write-Fail "IngestDailyIndex type is '$($dailyIndex.type)'" }
            if (Test-ReferenceName $dailyIndex.linkedServiceName $FunctionLinkedServiceName) { Write-Pass "IngestDailyIndex uses $FunctionLinkedServiceName" } else { Write-Fail "IngestDailyIndex does not reference $FunctionLinkedServiceName" }
            if ([string]$dailyIndex.typeProperties.functionName -eq "ingest_daily_index") { Write-Pass "IngestDailyIndex functionName is ingest_daily_index" } else { Write-Fail "IngestDailyIndex functionName is '$($dailyIndex.typeProperties.functionName)'" }
        }

        foreach ($activityName in @("IngestSubmissions", "IngestCompanyFacts")) {
            $activity = $pipelineProperties.activities | Where-Object { $_.name -eq $activityName } | Select-Object -First 1
            if ($null -eq $activity) {
                Write-Fail "Pipeline activity '$activityName' is missing"
                continue
            }
            $activityProperties = if ($activity.PSObject.Properties.Name -contains "typeProperties") { $activity.typeProperties } else { $activity }
            if ([string]$activity.type -eq "Custom") { Write-Pass "$activityName uses Custom activity" } else { Write-Fail "$activityName type is '$($activity.type)'" }
            if (Test-ReferenceName $activity.linkedServiceName "AzureBatchLS") { Write-Pass "$activityName uses AzureBatchLS" } else { Write-Fail "$activityName does not reference AzureBatchLS" }
            if (Test-ReferenceName $activityProperties.resourceLinkedService "AzureStorageLS") { Write-Pass "$activityName stages resources from AzureStorageLS" } else { Write-Fail "$activityName is missing AzureStorageLS staging" }
            if ([string]$activityProperties.folderPath -eq "$Container/adf-resources") { Write-Pass "$activityName folderPath is '$Container/adf-resources'" } else { Write-Fail "$activityName folderPath is '$($activityProperties.folderPath)'" }
            if ([string]$activityProperties.command.value -match "FULL_REFRESH") { Write-Pass "$activityName command carries fullRefresh into Batch" } else { Write-Fail "$activityName command does not carry fullRefresh into Batch" }
        }
    }

    $trigger = Get-AzJsonOrNull @("datafactory", "trigger", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--name", $TriggerName, "--output", "json")
    if ($null -eq $trigger) {
        Write-Fail "Trigger '$TriggerName' not found"
    } else {
        Write-Pass "Trigger '$TriggerName' exists"
        if ([string]$trigger.properties.runtimeState -eq "Started") { Write-Pass "Trigger '$TriggerName' is started" } else { Write-Warn "Trigger '$TriggerName' runtimeState is '$($trigger.properties.runtimeState)'" }
        if ([string]$trigger.properties.pipeline.pipelineReference.referenceName -eq $PipelineName) { Write-Pass "Daily trigger targets pipeline '$PipelineName'" } else { Write-Fail "Daily trigger points to '$($trigger.properties.pipeline.pipelineReference.referenceName)'" }
        if ($trigger.properties.pipeline.parameters.fullRefresh -eq $false) { Write-Pass "Daily trigger sets fullRefresh=false" } else { Write-Fail "Daily trigger fullRefresh is not false" }
    }

    $monthlyTrigger = Get-AzJsonOrNull @("datafactory", "trigger", "show", "--factory-name", $AdfName, "--resource-group", $ResourceGroup, "--name", $MonthlyTriggerName, "--output", "json")
    if ($null -eq $monthlyTrigger) {
        Write-Fail "Trigger '$MonthlyTriggerName' not found"
    } else {
        Write-Pass "Trigger '$MonthlyTriggerName' exists"
        if ([string]$monthlyTrigger.properties.runtimeState -eq "Started") { Write-Pass "Trigger '$MonthlyTriggerName' is started" } else { Write-Warn "Trigger '$MonthlyTriggerName' runtimeState is '$($monthlyTrigger.properties.runtimeState)'" }
        if ([string]$monthlyTrigger.properties.pipelines[0].pipelineReference.referenceName -eq $PipelineName) { Write-Pass "Monthly trigger targets pipeline '$PipelineName'" } else { Write-Fail "Monthly trigger points to '$($monthlyTrigger.properties.pipelines[0].pipelineReference.referenceName)'" }
        if ($monthlyTrigger.properties.pipelines[0].parameters.fullRefresh -eq $true) { Write-Pass "Monthly trigger sets fullRefresh=true" } else { Write-Fail "Monthly trigger fullRefresh is not true" }
    }
}

Write-Hdr "6. Managed Identity RBAC"
$identity = Get-AzJsonOrNull @("identity", "show", "--name", $ManagedIdentity, "--resource-group", $ResourceGroup, "--output", "json")
$storageScope = "/subscriptions/$subscription/resourceGroups/$ResourceGroup/providers/Microsoft.Storage/storageAccounts/$StorageAccount"
$batchScope = "/subscriptions/$subscription/resourceGroups/$ResourceGroup/providers/Microsoft.Batch/batchAccounts/$BatchAccount"
if ($null -eq $identity) {
    Write-Fail "Managed identity '$ManagedIdentity' not found"
} else {
    Write-Pass "Managed identity '$ManagedIdentity' found"
    $storageRbacEntries = Get-AzJsonOrNull @(
        "role", "assignment", "list",
        "--assignee-object-id", [string]$identity.principalId,
        "--role", "Storage Blob Data Contributor",
        "--scope", $storageScope,
        "--output", "json"
    )
    if (@($storageRbacEntries).Count -ge 1) {
        Write-Pass "Batch UAMI has Storage Blob Data Contributor on the storage account"
    } else {
        Write-Fail "Batch UAMI is missing Storage Blob Data Contributor on the storage account"
    }
}

if ($null -ne $functionApp -and $null -ne $functionApp.identity) {
    $functionStorageRbacEntries = Get-AzJsonOrNull @(
        "role", "assignment", "list",
        "--assignee-object-id", [string]$functionApp.identity.principalId,
        "--role", "Storage Blob Data Contributor",
        "--scope", $storageScope,
        "--output", "json"
    )
    if (@($functionStorageRbacEntries).Count -ge 1) {
        Write-Pass "Function App MI has Storage Blob Data Contributor on the storage account"
    } else {
        Write-Fail "Function App MI is missing Storage Blob Data Contributor on the storage account"
    }
}

if ($null -ne $adf -and $null -ne $adf.identity) {
    $adfBatchRbacEntries = Get-AzJsonOrNull @(
        "role", "assignment", "list",
        "--assignee-object-id", [string]$adf.identity.principalId,
        "--role", "Contributor",
        "--scope", $batchScope,
        "--output", "json"
    )
    if (@($adfBatchRbacEntries).Count -ge 1) {
        Write-Pass "ADF system MI has Contributor on the Batch account"
    } else {
        Write-Fail "ADF system MI is missing Contributor on the Batch account"
    }
}

Write-Warn "AcrPull is intentionally not required for the host-executed runtime"

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  PASS: $Pass   FAIL: $Fail   WARN: $Warn" -ForegroundColor White
Write-Host "======================================" -ForegroundColor Cyan

if ($Fail -gt 0) { exit 1 } else { exit 0 }
