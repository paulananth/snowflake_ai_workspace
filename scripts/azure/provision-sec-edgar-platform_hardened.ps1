<#
.SYNOPSIS
Provision Azure resources for the SEC EDGAR platform using an idempotent Azure CLI workflow.
#>

[CmdletBinding()]
param(
  [string]$SubscriptionId = $(if ($env:AZURE_SUBSCRIPTION_ID) { $env:AZURE_SUBSCRIPTION_ID } else { "" }),
  [string]$ResourceGroup = $(if ($env:AZURE_RESOURCE_GROUP) { $env:AZURE_RESOURCE_GROUP } else { "my-sec-edgar-rg" }),
  [string]$Location = $(if ($env:AZURE_LOCATION) { $env:AZURE_LOCATION } else { "eastus" }),
  [ValidatePattern('^[a-z0-9]{3,24}$')]
  [string]$StorageAccount = $(if ($env:AZURE_STORAGE_ACCOUNT) { $env:AZURE_STORAGE_ACCOUNT } else { "mysecedgarstorage" }),
  [string]$Container = $(if ($env:AZURE_CONTAINER) { $env:AZURE_CONTAINER } else { "sec-edgar" }),
  [string]$Prefix = $(if ($env:STORAGE_PREFIX) { $env:STORAGE_PREFIX } else { "sec-edgar" }),
  [string]$BatchAccount = $(if ($env:AZURE_BATCH_ACCOUNT) { $env:AZURE_BATCH_ACCOUNT } else { "mysecedgarbatch" }),
  [string]$AcrName = $(if ($env:AZURE_ACR_NAME) { $env:AZURE_ACR_NAME } else { "" }),
  [string]$AdfName = $(if ($env:AZURE_DATA_FACTORY_NAME) { $env:AZURE_DATA_FACTORY_NAME } else { "mysecedgaradf" }),
  [string]$IdentityName = $(if ($env:AZURE_MANAGED_IDENTITY_NAME) { $env:AZURE_MANAGED_IDENTITY_NAME } else { "sec-edgar-ingest-identity" }),
  [string]$BatchPoolId = $(if ($env:AZURE_BATCH_POOL_ID) { $env:AZURE_BATCH_POOL_ID } else { "sec-edgar-pool" }),
  [string]$VmSize = "Standard_D2s_v3",
  [Alias("BuildAndPushImage")]
  [switch]$BuildLegacyDockerArtifact,
  [string]$DockerImageName = "sec-edgar-ingest",
  [string]$DockerImageTag = "latest",
  [switch]$AutoApprove
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$PoolImagePublisher = "microsoft-dsvm"
$PoolImageOffer = "ubuntu-hpc"
$PoolImageSku = "2204"
$PoolImageVersion = "latest"
$PoolNodeAgentSkuId = "batch.node.ubuntu 22.04"
$PoolAutoScaleFormula = "startingNumberOfVMs = 0; maxNumberofVMs = 1; pendingTaskSamplePercent = `$PendingTasks.GetSamplePercent(180 * TimeInterval_Second); pendingTaskSamples = pendingTaskSamplePercent < 70 ? startingNumberOfVMs : avg(`$PendingTasks.GetSample(180 * TimeInterval_Second)); `$TargetDedicatedNodes = min(maxNumberofVMs, pendingTaskSamples); `$TargetLowPriorityNodes = 0; `$NodeDeallocationOption = taskcompletion;"
$UseLegacyDockerArtifact = $BuildLegacyDockerArtifact.IsPresent
$AzureCliExe = $null
$AzureCliResolved = $false
$UsePythonAzCli = $false

if ($UseLegacyDockerArtifact -and [string]::IsNullOrWhiteSpace($AcrName)) {
  throw "AcrName is required when -BuildLegacyDockerArtifact is set."
}

function Write-Section([string]$Title) {
  Write-Host ""
  Write-Host $Title -ForegroundColor Yellow
}

function Write-Status([string]$Message, [string]$Status, [ConsoleColor]$Color) {
  Write-Host ("  {0}  [{1}]" -f $Message, $Status) -ForegroundColor $Color
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

  $effectiveArgs = [System.Collections.Generic.List[string]]::new()
  foreach ($arg in $Args) {
    [void]$effectiveArgs.Add($arg)
  }
  if ($effectiveArgs -notcontains "--only-show-errors") {
    [void]$effectiveArgs.Add("--only-show-errors")
  }

  Resolve-AzRunner
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

function Ensure-AzureContext {
  $account = Invoke-Az -Args @("account", "show", "--query", "{name:name,id:id}", "--output", "json") -Json
  if (-not $account) {
    throw "Not logged in. Run: az login"
  }
  if (-not [string]::IsNullOrWhiteSpace($SubscriptionId)) {
    Invoke-Az -Args @("account", "set", "--subscription", $SubscriptionId, "--output", "none") | Out-Null
  } else {
    $script:SubscriptionId = [string]$account.id
  }
  Write-Status "Subscription: $($account.name) ($SubscriptionId)" "OK" Green
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

function Ensure-ResourceGroup {
  Write-Section "[1/8] Ensuring resource group..."
  Invoke-Az -Args @("group", "create", "--name", $ResourceGroup, "--location", $Location, "--output", "none") | Out-Null
  Write-Status "Resource group: $ResourceGroup" "OK" Green
}

function Ensure-Storage {
  Write-Section "[2/8] Ensuring ADLS Gen2 storage..."
  Invoke-Az -Args @(
    "storage", "account", "create",
    "--name", $StorageAccount,
    "--resource-group", $ResourceGroup,
    "--location", $Location,
    "--sku", "Standard_LRS",
    "--kind", "StorageV2",
    "--enable-hierarchical-namespace", "true",
    "--https-only", "true",
    "--min-tls-version", "TLS1_2",
    "--allow-blob-public-access", "false",
    "--access-tier", "Hot",
    "--output", "none"
  ) | Out-Null

  $fsExists = Invoke-Az -Args @(
    "storage", "fs", "exists",
    "--name", $Container,
    "--account-name", $StorageAccount,
    "--auth-mode", "login",
    "--query", "exists",
    "--output", "tsv"
  )
  if ($fsExists -ne "true") {
    Invoke-Az -Args @(
      "storage", "fs", "create",
      "--name", $Container,
      "--account-name", $StorageAccount,
      "--auth-mode", "login",
      "--output", "none"
    ) | Out-Null
  }

  $policy = @{
    rules = @(
      @{
        name = "bronze-to-cool"
        enabled = $true
        type = "Lifecycle"
        definition = @{
          filters = @{
            blobTypes = @("blockBlob")
            prefixMatch = @("$Prefix/bronze/")
          }
          actions = @{
            baseBlob = @{
              tierToCool = @{ daysAfterModificationGreaterThan = 30 }
              tierToArchive = @{ daysAfterModificationGreaterThan = 365 }
            }
          }
        }
      }
    )
  }
  $policyFile = [System.IO.Path]::GetTempFileName()
  try {
    [System.IO.File]::WriteAllText($policyFile, ($policy | ConvertTo-Json -Depth 20 -Compress), [System.Text.Encoding]::UTF8)
    Invoke-Az -Args @(
      "storage", "account", "management-policy", "create",
      "--account-name", $StorageAccount,
      "--resource-group", $ResourceGroup,
      "--policy", "@$policyFile",
      "--output", "none"
    ) | Out-Null
  } finally {
    Remove-Item -LiteralPath $policyFile -Force -ErrorAction SilentlyContinue
  }

  Write-Status "Storage account and filesystem ready" "OK" Green
}

function Ensure-LegacyAcr {
  Write-Section "[3/8] Ensuring optional legacy ACR..."
  if (-not $UseLegacyDockerArtifact) {
    Write-Status "Legacy Docker artifact path disabled" "SKIPPED" DarkGray
    return $null
  }

  Invoke-Az -Args @(
    "acr", "create",
    "--name", $AcrName,
    "--resource-group", $ResourceGroup,
    "--location", $Location,
    "--sku", "Basic",
    "--admin-enabled", "false",
    "--output", "none"
  ) | Out-Null
  $acr = Invoke-Az -Args @(
    "acr", "show",
    "--name", $AcrName,
    "--resource-group", $ResourceGroup,
    "--output", "json"
  ) -Json
  Write-Status "Legacy ACR ready: $($acr.loginServer)" "OK" Green
  return $acr
}

function Ensure-IdentityAndRbac {
  param([object]$Acr)

  Write-Section "[4/8] Ensuring identity and RBAC..."
  Invoke-Az -Args @(
    "identity", "create",
    "--name", $IdentityName,
    "--resource-group", $ResourceGroup,
    "--location", $Location,
    "--output", "none"
  ) | Out-Null

  $identity = Invoke-Az -Args @(
    "identity", "show",
    "--name", $IdentityName,
    "--resource-group", $ResourceGroup,
    "--output", "json"
  ) -Json

  $storageScope = Invoke-Az -Args @(
    "storage", "account", "show",
    "--name", $StorageAccount,
    "--resource-group", $ResourceGroup,
    "--query", "id",
    "--output", "tsv"
  )
  Ensure-RoleAssignment ([string]$identity.principalId) $storageScope "Storage Blob Data Contributor" "Managed identity -> Storage"
  return $identity
}

function Ensure-BatchAccount {
  Write-Section "[5/8] Ensuring Azure Batch account..."
  Invoke-Az -Args @(
    "batch", "account", "create",
    "--name", $BatchAccount,
    "--resource-group", $ResourceGroup,
    "--location", $Location,
    "--storage-account", $StorageAccount,
    "--output", "none"
  ) | Out-Null
  $batchScope = Invoke-Az -Args @(
    "batch", "account", "show",
    "--name", $BatchAccount,
    "--resource-group", $ResourceGroup,
    "--query", "id",
    "--output", "tsv"
  )
  Write-Status "Batch account: $BatchAccount" "OK" Green
  return $batchScope
}

function Ensure-AdfAndRbac {
  param([string]$BatchScope)

  Write-Section "[6/8] Ensuring Azure Data Factory..."
  Invoke-Az -Args @(
    "datafactory", "create",
    "--factory-name", $AdfName,
    "--resource-group", $ResourceGroup,
    "--location", $Location,
    "--output", "none"
  ) | Out-Null
  $principalId = Invoke-Az -Args @(
    "datafactory", "show",
    "--factory-name", $AdfName,
    "--resource-group", $ResourceGroup,
    "--query", "identity.principalId",
    "--output", "tsv"
  )
  Ensure-RoleAssignment $principalId $BatchScope "Contributor" "ADF system MI -> Batch"
  Write-Status "ADF ready: $AdfName" "OK" Green
}

function Ensure-BatchPool {
  param([object]$Identity)

  Write-Section "[7/8] Ensuring non-container Batch pool..."
  $poolDefinition = @{
    identity = @{
      type = "UserAssigned"
      userAssignedIdentities = @{
        ([string]$Identity.id) = @{}
      }
    }
    properties = @{
      vmSize = $VmSize.ToUpperInvariant()
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

  $poolUri = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Batch/batchAccounts/$BatchAccount/pools/${BatchPoolId}?api-version=2025-06-01"
  $probe = Invoke-Az -Args @("rest", "--method", "get", "--uri", $poolUri, "--output", "none") -AllowFailure
  if ($probe.Success) {
    Write-Status "Batch pool already exists: $BatchPoolId" "SKIPPED" DarkGray
    return
  }

  $poolFile = [System.IO.Path]::GetTempFileName()
  try {
    [System.IO.File]::WriteAllText($poolFile, ($poolDefinition | ConvertTo-Json -Depth 25), [System.Text.Encoding]::UTF8)
    Invoke-Az -Args @(
      "rest", "--method", "put",
      "--uri", $poolUri,
      "--headers", "If-None-Match=*",
      "--body", "@$poolFile",
      "--output", "none"
    ) | Out-Null
  } finally {
    Remove-Item -LiteralPath $poolFile -Force -ErrorAction SilentlyContinue
  }

  Write-Status "Batch pool created: $BatchPoolId" "OK" Green
}

function Publish-LegacyArtifact {
  Write-Section "[8/8] Optional legacy Docker artifact build..."
  if (-not $UseLegacyDockerArtifact) {
    Write-Status "Legacy Docker artifact path disabled" "SKIPPED" DarkGray
    return
  }
  Invoke-Az -Args @(
    "acr", "build",
    "--registry", $AcrName,
    "--image", "$DockerImageName`:$DockerImageTag",
    (Split-Path $PSScriptRoot -Parent | Split-Path -Parent)
  ) | Out-Null
  Write-Status "Built legacy image: $AcrName.azurecr.io/$DockerImageName`:$DockerImageTag" "OK" Green
}

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  SEC EDGAR Platform Provisioning" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  Resource Group   = $ResourceGroup"
Write-Host "  Location         = $Location"
Write-Host "  Storage Account  = $StorageAccount"
Write-Host "  Batch Account    = $BatchAccount"
Write-Host "  ADF              = $AdfName"
Write-Host "  Managed Identity = $IdentityName"
Write-Host "  Legacy ACR       = $(if ([string]::IsNullOrWhiteSpace($AcrName)) { '<disabled>' } else { $AcrName })"
Write-Host "=================================================================" -ForegroundColor Cyan

Ensure-AzureContext
Ensure-ResourceGroup
Ensure-Storage
$acr = Ensure-LegacyAcr
$identity = Ensure-IdentityAndRbac -Acr $acr
$batchScope = Ensure-BatchAccount
Ensure-AdfAndRbac -BatchScope $batchScope
Ensure-BatchPool -Identity $identity
Publish-LegacyArtifact

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  PROVISIONING COMPLETE" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  ADLS URL     : abfss://$Container@$StorageAccount.dfs.core.windows.net/$Prefix"
Write-Host "  Batch Pool   : $BatchPoolId"
Write-Host "  ADF          : $AdfName"
Write-Host "  Legacy ACR   : $(if ($UseLegacyDockerArtifact) { $AcrName } else { '<disabled>' })"
Write-Host "=================================================================" -ForegroundColor Green
