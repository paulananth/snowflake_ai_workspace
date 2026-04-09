<#
.SYNOPSIS
Provision Azure resources for the SEC EDGAR platform (per docs/azure_ad_setup.md).

.DESCRIPTION
Creates Azure components used by the SEC EDGAR ingestion + transform pipeline:
  - Resource group
  - ADLS Gen2 storage account (HNS enabled) + filesystem
  - Storage lifecycle management policy for Bronze -> Cool/Archive
  - Optional Azure Container Registry (ACR) for the legacy Docker artifact path
  - User-assigned managed identity (for Batch nodes) + RBAC assignments
  - Azure Batch account + autoscaled pool with dedicated capacity (scale-to-zero)
  - Azure Data Factory (ADF) + RBAC assignments

This script uses the Azure CLI (`az`) and is designed to be re-runnable (best-effort idempotent).

IMPORTANT NOTES (from the guide)
  - The current ADF Custom Activity design runs on the Batch VM host, not inside a container task.
    Recreate the pool without `containerConfiguration`.
  - If you attach multiple identities to a node, set AZURE_CLIENT_ID on the host runtime to the
    user-assigned identity clientId.

.PARAMETER SubscriptionId
Azure subscription ID. If omitted, uses `az account show`.

.PARAMETER ResourceGroup
Resource group name.

.PARAMETER Location
Azure region (e.g. eastus, westus2).

.PARAMETER StorageAccount
Storage account name (globally unique, lowercase, 3-24 chars).

.PARAMETER Container
ADLS Gen2 filesystem name (container).

.PARAMETER Prefix
Key prefix for blob paths (e.g. sec-edgar). Used for lifecycle policy prefixMatch "<prefix>/bronze/".

.PARAMETER BatchAccount
Azure Batch account name.

.PARAMETER AcrName
Azure Container Registry name. Optional unless -BuildLegacyDockerArtifact is set.

.PARAMETER AdfName
Azure Data Factory name.

.PARAMETER IdentityName
User-assigned managed identity name. Default: sec-edgar-ingest-identity

.PARAMETER BatchPoolId
Batch pool id. Default: sec-edgar-pool

.PARAMETER VmSize
Batch VM size. Default: Standard_D2s_v3

.PARAMETER BuildLegacyDockerArtifact
If set, creates the optional ACR (when needed) and runs `az acr build` for the legacy
`sec-edgar-ingest:latest` artifact.

.PARAMETER DockerImageName
Docker image repo/name (without registry). Default: sec-edgar-ingest

.PARAMETER DockerImageTag
Docker tag. Default: latest

.PARAMETER AutoApprove
If set, skips interactive prompts and runs all steps.
#>

[CmdletBinding()]
param(
  [string]$SubscriptionId = $(if ($env:AZURE_SUBSCRIPTION_ID) { $env:AZURE_SUBSCRIPTION_ID } else { "" }),
  [string]$ResourceGroup = $(if ($env:AZURE_RESOURCE_GROUP) { $env:AZURE_RESOURCE_GROUP } else { "my-sec-edgar-rg" }),
  [string]$Location = $(if ($env:AZURE_LOCATION) { $env:AZURE_LOCATION } else { "eastus" }),
  [string]$StorageAccount = $(if ($env:AZURE_STORAGE_ACCOUNT) { $env:AZURE_STORAGE_ACCOUNT } else { "mysecedgarstorage" }),
  [string]$Container = $(if ($env:AZURE_CONTAINER) { $env:AZURE_CONTAINER } else { "sec-edgar" }),
  [string]$Prefix = $(if ($env:STORAGE_PREFIX) { $env:STORAGE_PREFIX } else { "sec-edgar" }),
  [string]$BatchAccount = $(if ($env:AZURE_BATCH_ACCOUNT) { $env:AZURE_BATCH_ACCOUNT } else { "mysecedgarbatch" }),
  [string]$AcrName = $(if ($env:AZURE_ACR_NAME) { $env:AZURE_ACR_NAME } else { "" }),
  [string]$AdfName = $(if ($env:AZURE_DATA_FACTORY_NAME) { $env:AZURE_DATA_FACTORY_NAME } else { "mysecedgaradf" }),
  [string]$IdentityName = $(if ($env:AZURE_MANAGED_IDENTITY_NAME) { $env:AZURE_MANAGED_IDENTITY_NAME } else { "sec-edgar-ingest-identity" }),
  [string]$BatchPoolId = $(if ($env:AZURE_BATCH_POOL_ID) { $env:AZURE_BATCH_POOL_ID } else { "sec-edgar-pool" }),
  [string]$VmSize = "Standard_D2s_v3",
  [Alias('BuildAndPushImage')]
  [switch]$BuildLegacyDockerArtifact,
  [string]$DockerImageName = 'sec-edgar-ingest',
  [string]$DockerImageTag = 'latest',
  [switch]$AutoApprove
)

& (Join-Path $PSScriptRoot "provision-sec-edgar-platform_hardened.ps1") @PSBoundParameters
return

function Confirm-Step {
  param(
    [Parameter(Mandatory = $true)][string]$Title,
    [Parameter(Mandatory = $true)][string]$Explanation,
    [Parameter(Mandatory = $true)][string]$CommandPreview
  )

  Write-Host "`n=== $Title ===" -ForegroundColor Cyan
  Write-Host $Explanation -ForegroundColor Gray
  Write-Host "`nCommand:" -ForegroundColor DarkGray
  Write-Host $CommandPreview -ForegroundColor DarkGray

  if ($AutoApprove) {
    Write-Host "AutoApprove enabled: proceeding." -ForegroundColor DarkGreen
    return 'proceed'
  }

  $resp = Read-Host "Proceed? ([Enter]/Y/n/s=skip)"
  if ($resp -in @('n', 'N', 'no', 'NO')) {
    throw "Aborted by user at step: $Title"
  }
  if ($resp -in @('s', 'S', 'skip', 'SKIP')) {
    Write-Host "Skipped: $Title" -ForegroundColor Yellow
    return 'skip'
  }

  return 'proceed'
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Required command not found on PATH: $Name"
  }
}

function New-SkipResult {
  [PSCustomObject]@{
    Skipped = $true
  }
}

function Test-StepSkipped {
  param([Parameter(Mandatory = $false)]$Value)

  return ($null -ne $Value -and $Value.PSObject.Properties.Name -contains 'Skipped' -and $Value.Skipped -eq $true)
}

function Get-TrimmedString {
  param([Parameter(Mandatory = $false)]$Value)

  if ($null -eq $Value) {
    return $null
  }

  return ([string]$Value).Trim()
}

function Invoke-Az {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args,
    [Parameter(Mandatory = $false)][switch]$Json,
    [Parameter(Mandatory = $false)][string]$Title,
    [Parameter(Mandatory = $false)][string]$Explanation
  )

  $effectiveArgs = [System.Collections.Generic.List[string]]::new()
  foreach ($arg in $Args) {
    [void]$effectiveArgs.Add($arg)
  }
  if ($effectiveArgs -notcontains '--only-show-errors') {
    [void]$effectiveArgs.Add('--only-show-errors')
  }

  $cmd = @($AzCommand) + $effectiveArgs
  if ($Title) {
    $stepExplanation = ''
    if ($null -ne $Explanation) {
      $stepExplanation = $Explanation
    }
    $stepDecision = Confirm-Step -Title $Title -Explanation $stepExplanation -CommandPreview ('az ' + ($effectiveArgs -join ' '))
    if ($stepDecision -eq 'skip') {
      return (New-SkipResult)
    }
  } else {
    Write-Host ("`n> " + ($cmd -join ' ')) -ForegroundColor DarkGray
  }

  $stdoutPath = [System.IO.Path]::GetTempFileName()
  $stderrPath = [System.IO.Path]::GetTempFileName()
  try {
    & $AzCommand @effectiveArgs 1> $stdoutPath 2> $stderrPath
    $exitCode = $LASTEXITCODE

    $stdout = if (Test-Path -LiteralPath $stdoutPath) {
      [System.IO.File]::ReadAllText($stdoutPath)
    } else {
      ''
    }

    $stderr = if (Test-Path -LiteralPath $stderrPath) {
      [System.IO.File]::ReadAllText($stderrPath)
    } else {
      ''
    }

    if ($exitCode -ne 0) {
      $details = @()
      if (-not [string]::IsNullOrWhiteSpace($stdout)) { $details += $stdout.TrimEnd() }
      if (-not [string]::IsNullOrWhiteSpace($stderr)) { $details += $stderr.TrimEnd() }
      $detailText = $details -join [Environment]::NewLine
      throw "Azure CLI command failed (exit $exitCode): az $($effectiveArgs -join ' ')`n$detailText"
    }

    if (-not [string]::IsNullOrWhiteSpace($stderr)) {
      Write-Host $stderr.TrimEnd() -ForegroundColor Yellow
    }

    $out = $stdout
  } finally {
    if (Test-Path -LiteralPath $stdoutPath) {
      Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $stderrPath) {
      Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    }
  }
  if ($Json) {
    if ([string]::IsNullOrWhiteSpace($out)) { return $null }
    return ($out | ConvertFrom-Json)
  }
  return $out
}

function Wait-ForAzValue {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args,
    [Parameter(Mandatory = $true)][string]$Title,
    [Parameter(Mandatory = $true)][string]$Explanation,
    [Parameter(Mandatory = $true)][string]$PendingMessage,
    [Parameter(Mandatory = $false)][int]$MaxAttempts = 20,
    [Parameter(Mandatory = $false)][int]$DelaySeconds = 15
  )

  $result = Invoke-Az -Args $Args -Title $Title -Explanation $Explanation
  if (Test-StepSkipped $result) {
    return $result
  }

  $value = Get-TrimmedString $result
  if (-not [string]::IsNullOrWhiteSpace($value)) {
    return $value
  }

  for ($attempt = 2; $attempt -le $MaxAttempts; $attempt++) {
    Write-Host "$PendingMessage Waiting $DelaySeconds seconds before retry $attempt of $MaxAttempts." -ForegroundColor Yellow
    Start-Sleep -Seconds $DelaySeconds

    $retryResult = Invoke-Az -Args $Args
    $value = Get-TrimmedString $retryResult
    if (-not [string]::IsNullOrWhiteSpace($value)) {
      return $value
    }
  }

  throw "$Title did not return a value after $MaxAttempts attempts."
}

function Ensure-RoleAssignment {
  param(
    [Parameter(Mandatory = $true)][string]$Role,
    [Parameter(Mandatory = $true)][string]$AssigneeObjectId,
    [Parameter(Mandatory = $true)][ValidateSet('ServicePrincipal','User')][string]$AssigneePrincipalType,
    [Parameter(Mandatory = $true)][string]$Scope
  )

  $existing = Invoke-Az -Args @(
    'role','assignment','list',
    '--assignee-object-id', $AssigneeObjectId,
    '--scope', $Scope,
    '--query', "[?roleDefinitionName=='$Role']",
    '--output','json'
  ) -Json -Title "Check RBAC: $Role" -Explanation "Checks whether the role assignment already exists so the script can be re-run safely."

  if (Test-StepSkipped $existing) {
    return
  }

  $existingAssignments = @($existing)
  if ($existingAssignments.Count -gt 0) {
    Write-Host "Role assignment already exists: $Role on $Scope" -ForegroundColor DarkGreen
    return
  }

  $createResult = Invoke-Az -Args @(
    'role','assignment','create',
    '--role', $Role,
    '--assignee-object-id', $AssigneeObjectId,
    '--assignee-principal-type', $AssigneePrincipalType,
    '--scope', $Scope
  ) -Title "Create RBAC: $Role" -Explanation "Creates the RBAC role assignment required for the pipeline to access resources."
  if (Test-StepSkipped $createResult) {
    return
  }
}

Assert-CommandExists -Name 'az'
$AzCommand = (Get-Command az -ErrorAction Stop).Source

if (-not $SubscriptionId) {
  $SubscriptionId = (Invoke-Az -Args @('account','show','--query','id','--output','tsv') -Title 'Detect subscription' -Explanation 'Reads the active Azure subscription ID from your current az login context.').Trim()
}
Invoke-Az -Args @('account','set','--subscription', $SubscriptionId) -Title 'Select subscription' -Explanation 'Sets the subscription used for all subsequent resource creation.' | Out-Null

Write-Host "`n=== Inputs ===" -ForegroundColor Cyan
Write-Host "SubscriptionId : $SubscriptionId"
Write-Host "RG             : $ResourceGroup"
Write-Host "Location       : $Location"
Write-Host "StorageAccount : $StorageAccount"
Write-Host "Container      : $Container"
Write-Host "Prefix         : $Prefix"
Write-Host "BatchAccount   : $BatchAccount"
Write-Host "BatchPoolId    : $BatchPoolId"
Write-Host "ACR            : $(if ([string]::IsNullOrWhiteSpace($AcrName)) { '<disabled>' } else { $AcrName })"
Write-Host "ADF            : $AdfName"
Write-Host "Identity       : $IdentityName"

# Step 1 — Resource group
Invoke-Az -Args @('group','create','--name', $ResourceGroup,'--location', $Location) -Title 'Create resource group' -Explanation 'Creates (or updates) the resource group that will contain all resources.' | Out-Null

# Step 2 — ADLS Gen2 storage account + filesystem
Invoke-Az -Args @(
  'storage','account','create',
  '--name', $StorageAccount,
  '--resource-group', $ResourceGroup,
  '--location', $Location,
  '--sku','Standard_LRS',
  '--kind','StorageV2',
  '--enable-hierarchical-namespace','true',
  '--https-only','true',
  '--min-tls-version','TLS1_2',
  '--allow-blob-public-access','false',
  '--access-tier','Hot'
) -Title 'Create ADLS Gen2 storage account' -Explanation 'Creates the StorageV2 account with Hierarchical Namespace enabled (required for ADLS Gen2 and abfss:// paths).' | Out-Null

$fsExistsResult = Invoke-Az -Args @(
  'storage','fs','exists',
  '--name', $Container,
  '--account-name', $StorageAccount,
  '--auth-mode','login',
  '--query','exists',
  '--output','tsv'
) -Title 'Check ADLS filesystem exists' -Explanation 'Checks whether the ADLS Gen2 filesystem already exists so the script can be re-run safely.'

$fsExists = if (Test-StepSkipped $fsExistsResult) { $null } else { $fsExistsResult.Trim() }

if (Test-StepSkipped $fsExistsResult) {
  Write-Host "Skipped ADLS filesystem block." -ForegroundColor Yellow
} elseif ($fsExists -eq 'true') {
  Write-Host "ADLS filesystem already exists: $Container" -ForegroundColor DarkGreen
} else {
  Invoke-Az -Args @(
    'storage','fs','create',
    '--name', $Container,
    '--account-name', $StorageAccount,
    '--auth-mode','login'
  ) -Title 'Create ADLS filesystem' -Explanation 'Creates the ADLS Gen2 filesystem (container) used for bronze/silver/gold data.' | Out-Null
}

$hns = (Invoke-Az -Args @(
  'storage','account','show',
  '--name', $StorageAccount,
  '--resource-group', $ResourceGroup,
  '--query','isHnsEnabled',
  '--output','tsv'
 ) -Title 'Verify HNS enabled' -Explanation 'Validates the storage account has Hierarchical Namespace enabled (cannot be enabled after creation).').Trim()
if ($hns -ne 'true') { throw "Storage account $StorageAccount does not have HNS enabled (isHnsEnabled=$hns)." }

# Step 2a — Lifecycle policy (bronze -> cool/archive)
$policy = @{
  rules = @(
    @{
      name    = 'bronze-to-cool'
      enabled = $true
      type    = 'Lifecycle'
      definition = @{
        filters = @{
          blobTypes   = @('blockBlob')
          prefixMatch = @("$Prefix/bronze/")
        }
        actions = @{
          baseBlob = @{
            tierToCool    = @{ daysAfterModificationGreaterThan = 30 }
            tierToArchive = @{ daysAfterModificationGreaterThan = 365 }
          }
        }
      }
    }
  )
}
$policyJson = ($policy | ConvertTo-Json -Depth 20 -Compress)
$policyFile = [System.IO.Path]::GetTempFileName()
try {
  [System.IO.File]::WriteAllText($policyFile, $policyJson, [System.Text.Encoding]::UTF8)
  Invoke-Az -Args @(
    'storage','account','management-policy','create',
    '--account-name', $StorageAccount,
    '--resource-group', $ResourceGroup,
    '--policy', "@$policyFile"
  ) -Title 'Apply lifecycle policy' -Explanation 'Applies a storage lifecycle policy to move bronze data to Cool after 30 days and Archive after 365 days.' | Out-Null
} finally {
  if (Test-Path -LiteralPath $policyFile) {
    Remove-Item -LiteralPath $policyFile -Force -ErrorAction SilentlyContinue
  }
}

# Step 3 — ACR (Basic)
Invoke-Az -Args @(
  'acr','create',
  '--name', $AcrName,
  '--resource-group', $ResourceGroup,
  '--location', $Location,
  '--sku','Basic',
  '--admin-enabled','false'
) -Title 'Create ACR' -Explanation 'Creates an Azure Container Registry (Basic) for hosting the ingestion container image.' | Out-Null

$acr = Invoke-Az -Args @('acr','show','--name', $AcrName,'--resource-group', $ResourceGroup,'--output','json') -Json -Title 'Read ACR details' -Explanation 'Fetches the ACR resource ID and login server for later RBAC and (optional) image push.'
if (Test-StepSkipped $acr) {
  Write-Host "Skipped ACR details block." -ForegroundColor Yellow
  $acrId = $null
  $acrServer = $null
} else {
  $acrId = $acr.id
  $acrServer = $acr.loginServer
}

# Step 4 — User-assigned managed identity
Invoke-Az -Args @(
  'identity','create',
  '--name', $IdentityName,
  '--resource-group', $ResourceGroup,
  '--location', $Location
) -Title 'Create user-assigned managed identity' -Explanation 'Creates a user-assigned managed identity that will be attached to Batch pool nodes.' | Out-Null

$identity = Invoke-Az -Args @(
  'identity','show',
  '--name', $IdentityName,
  '--resource-group', $ResourceGroup,
  '--output','json'
) -Json -Title 'Read managed identity details' -Explanation 'Fetches the identity resource ID, clientId, and principalId for RBAC and Batch pool attachment.'
if (Test-StepSkipped $identity) {
  Write-Host "Skipped managed identity details block." -ForegroundColor Yellow
  $identityId = $null
  $clientId = $null
  $principalId = $null
} else {
  $identityId = $identity.id
  $clientId = $identity.clientId
  $principalId = $identity.principalId
}

Write-Host "`n=== Identity outputs ===" -ForegroundColor Cyan
Write-Host "IDENTITY_ID   : $identityId"
Write-Host "CLIENT_ID     : $clientId (set as AZURE_CLIENT_ID on Batch pool nodes)"
Write-Host "PRINCIPAL_ID  : $principalId"

# Step 5 — RBAC roles for the managed identity
$storageScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Storage/storageAccounts/$StorageAccount"
if ([string]::IsNullOrWhiteSpace($principalId)) {
  Write-Host "Skipped managed identity RBAC block because the managed identity details are unavailable." -ForegroundColor Yellow
} else {
  Ensure-RoleAssignment -Role 'Storage Blob Data Contributor' -AssigneeObjectId $principalId -AssigneePrincipalType ServicePrincipal -Scope $storageScope
}

# Step 6 — Azure Batch account
Invoke-Az -Args @(
  'batch','account','create',
  '--name', $BatchAccount,
  '--resource-group', $ResourceGroup,
  '--location', $Location,
  '--storage-account', $StorageAccount
) -Title 'Create Azure Batch account' -Explanation 'Creates the Batch account used to run compute tasks on an autoscaled pool.' | Out-Null

$batchScopeResult = Invoke-Az -Args @(
  'batch','account','show',
  '--name', $BatchAccount,
  '--resource-group', $ResourceGroup,
  '--query','id',
  '--output','tsv'
) -Title 'Read Batch account resource ID' -Explanation 'Retrieves the Batch account ARM resource ID for RBAC assignments.'
if (Test-StepSkipped $batchScopeResult) {
  Write-Host "Skipped Batch account resource ID block." -ForegroundColor Yellow
  $batchScope = $null
} else {
  $batchScope = $batchScopeResult.Trim()
}

Invoke-Az -Args @('batch','account','login','--name', $BatchAccount,'--resource-group', $ResourceGroup) -Title 'Login to Batch account' -Explanation 'Sets the CLI context so subsequent az batch pool commands can run.' | Out-Null

# Step 7 — Azure Data Factory + principal ID
Invoke-Az -Args @(
  'datafactory','create',
  '--factory-name', $AdfName,
  '--resource-group', $ResourceGroup,
  '--location', $Location
) -Title 'Create Azure Data Factory' -Explanation 'Creates the Data Factory instance that will orchestrate the pipeline and submit Batch jobs.' | Out-Null

$adfPrincipalResult = Wait-ForAzValue -Args @(
  'datafactory','show',
  '--factory-name', $AdfName,
  '--resource-group', $ResourceGroup,
  '--query','identity.principalId',
  '--output','tsv'
) -Title 'Read ADF principal ID' -Explanation 'Retrieves the system-assigned managed identity principal ID for RBAC assignments.' -PendingMessage 'ADF managed identity is not ready yet.'
if (Test-StepSkipped $adfPrincipalResult) {
  Write-Host "Skipped ADF principal ID block." -ForegroundColor Yellow
  $adfPrincipal = $null
} else {
  $adfPrincipal = $adfPrincipalResult.Trim()
}

Write-Host "`nADF principal ID: $adfPrincipal" -ForegroundColor Cyan

# Step 8 - RBAC for ADF identity (Batch)
if ([string]::IsNullOrWhiteSpace($adfPrincipal) -or [string]::IsNullOrWhiteSpace($batchScope)) {
  Write-Host "Skipped ADF RBAC block because the factory principal ID or Batch scope is unavailable." -ForegroundColor Yellow
} else {
  Ensure-RoleAssignment -Role 'Contributor' -AssigneeObjectId $adfPrincipal -AssigneePrincipalType ServicePrincipal -Scope $batchScope
}

# Step 9 — Batch pool (cost-optimized; scale-to-zero)
$autoScaleFormula = @'
startingNumberOfVMs = 0;
maxNumberofVMs = 1;
pendingTaskSamplePercent = $PendingTasks.GetSamplePercent(180 * TimeInterval_Second);
pendingTaskSamples = pendingTaskSamplePercent < 70
  ? startingNumberOfVMs
  : avg($PendingTasks.GetSample(180 * TimeInterval_Second));
$TargetDedicatedNodes = min(maxNumberofVMs, pendingTaskSamples);
$TargetLowPriorityNodes = 0;
$NodeDeallocationOption = taskcompletion;
'@ -replace "(\r?\n)+"," "

$poolExists = $false
try {
  $poolCheck = Invoke-Az -Args @(
    'batch','pool','show',
    '--pool-id', $BatchPoolId,
    '--account-name', $BatchAccount,
    '--output','none'
  ) -Title 'Check Batch pool exists' -Explanation 'Checks whether the Batch pool already exists (to avoid failing on recreate).'
  if (Test-StepSkipped $poolCheck) {
    Write-Host "Skipped Batch pool block." -ForegroundColor Yellow
  } else {
    $poolExists = $true
    Write-Host "Batch pool already exists: $BatchPoolId (will not recreate)." -ForegroundColor DarkGreen
  }
} catch {
  $poolExists = $false
}

if (-not $poolExists) {
  if ([string]::IsNullOrWhiteSpace($identityId)) {
    Write-Host "Skipped Batch pool create block because the managed identity resource ID is unavailable." -ForegroundColor Yellow
  } else {
    $poolDefinition = @{
      identity = @{
        type = 'UserAssigned'
        userAssignedIdentities = @{
          $identityId = @{}
        }
      }
      properties = @{
        vmSize = $VmSize
        deploymentConfiguration = @{
          virtualMachineConfiguration = @{
            imageReference = @{
              publisher = 'microsoft-dsvm'
              offer = 'ubuntu-hpc'
              sku = '2204'
              version = 'latest'
            }
            nodeAgentSkuId = 'batch.node.ubuntu 22.04'
          }
        }
        scaleSettings = @{
          autoScale = @{
            evaluationInterval = 'PT5M'
            formula = $autoScaleFormula
          }
        }
      }
    }

    $poolDefinitionJson = ($poolDefinition | ConvertTo-Json -Depth 20 -Compress)
    $poolDefinitionFile = [System.IO.Path]::GetTempFileName()
    try {
      [System.IO.File]::WriteAllText($poolDefinitionFile, $poolDefinitionJson, [System.Text.Encoding]::UTF8)
      Invoke-Az -Args @(
        'rest',
        '--method', 'put',
        '--uri', "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Batch/batchAccounts/$BatchAccount/pools/${BatchPoolId}?api-version=2025-06-01",
        '--headers', 'If-None-Match=*',
        '--body', "@$poolDefinitionFile"
      ) -Title 'Create Batch pool (dedicated autoscale)' -Explanation 'Creates the pool with autoscale enabled, dedicated Batch capacity, and a user-assigned managed identity using the Azure Batch management API.' | Out-Null
    } finally {
      if (Test-Path -LiteralPath $poolDefinitionFile) {
        Remove-Item -LiteralPath $poolDefinitionFile -Force -ErrorAction SilentlyContinue
      }
    }
  }
}

Write-Host "`nNOTE: The recommended ADF runtime is a non-container Batch pool that runs the staged task bundle on the host VM." -ForegroundColor Yellow
Write-Host "      Set AZURE_CLIENT_ID=$clientId on the host runtime environment if multiple managed identities are present." -ForegroundColor Yellow
Write-Host "      ACR login server: $acrServer (kept for the legacy image build/push workflow)." -ForegroundColor Yellow

# Step 10 — Optional: push Docker image to ACR
if ($BuildAndPushImage) {
  Assert-CommandExists -Name 'docker'

  Invoke-Az -Args @('acr','login','--name', $AcrName) -Title 'ACR login' -Explanation 'Authenticates Docker to the Azure Container Registry.' | Out-Null
  $imageUri = "$acrServer/$DockerImageName`:$DockerImageTag"

  Confirm-Step -Title 'Docker build' -Explanation 'Builds the container image from the current directory (requires Dockerfile).' -CommandPreview ("docker build -t $imageUri .")
  Write-Host "`nBuilding Docker image: $imageUri" -ForegroundColor Cyan
  & docker build -t $imageUri .
  if ($LASTEXITCODE -ne 0) { throw "docker build failed (exit $LASTEXITCODE)" }

  Confirm-Step -Title 'Docker push' -Explanation 'Pushes the built image to ACR so Batch nodes can pull it.' -CommandPreview ("docker push $imageUri")
  Write-Host "Pushing Docker image: $imageUri" -ForegroundColor Cyan
  & docker push $imageUri
  if ($LASTEXITCODE -ne 0) { throw "docker push failed (exit $LASTEXITCODE)" }

  Write-Host "Image URI: $imageUri" -ForegroundColor Green
}

Write-Host "`n=== Summary (record these) ===" -ForegroundColor Cyan
Write-Host "Storage account: $StorageAccount"
Write-Host "Container      : $Container"
Write-Host "Prefix         : $Prefix"
Write-Host "ADLS URL       : abfss://$Container@$StorageAccount.dfs.core.windows.net/$Prefix"
Write-Host "Managed identity resource ID: $identityId"
Write-Host "Managed identity client ID  : $clientId  (AZURE_CLIENT_ID on Batch pool)"
Write-Host "ACR login server            : $acrServer"
Write-Host "Docker image URI            : $acrServer/$DockerImageName`:$DockerImageTag"
Write-Host "Azure Data Factory name     : $AdfName"
Write-Host "Azure Batch account name    : $BatchAccount"
Write-Host "Batch pool id               : $BatchPoolId"

