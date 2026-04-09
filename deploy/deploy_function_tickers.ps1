[CmdletBinding()]
param(
    [string]$SubscriptionId = $(if ($env:AZURE_SUBSCRIPTION_ID) { $env:AZURE_SUBSCRIPTION_ID } else { "" }),
    [string]$ResourceGroup = $(if ($env:AZURE_RESOURCE_GROUP) { $env:AZURE_RESOURCE_GROUP } else { "my-sec-edgar-rg" }),
    [string]$Location = $(if ($env:AZURE_LOCATION) { $env:AZURE_LOCATION } else { "eastus" }),
    [string]$DataStorageAccount = $(if ($env:AZURE_STORAGE_ACCOUNT) { $env:AZURE_STORAGE_ACCOUNT } else { "mysecedgarstorage" }),
    [string]$FunctionStorageAccount = $(if ($env:AZURE_FUNCTION_STORAGE_ACCOUNT) { $env:AZURE_FUNCTION_STORAGE_ACCOUNT } else { "" }),
    [string]$Container = $(if ($env:AZURE_CONTAINER) { $env:AZURE_CONTAINER } else { "sec-edgar" }),
    [string]$Prefix = $(if ($env:STORAGE_PREFIX) { $env:STORAGE_PREFIX } else { "sec-edgar" }),
    [string]$AdfName = $(if ($env:AZURE_DATA_FACTORY_NAME) { $env:AZURE_DATA_FACTORY_NAME } else { "mysecedgaradf" }),
    [string]$FunctionAppName = $(if ($env:AZURE_FUNCTION_APP_NAME) { $env:AZURE_FUNCTION_APP_NAME } else { "" }),
    [string]$PipelineName = "sec-edgar-function-tickers-ingest",
    [string]$FunctionLinkedServiceName = "AzureFunctionTickersLS",
    [string]$IngestDate = "2026-04-09",
    [string]$AzConfigDir = $(if ($env:AZURE_CONFIG_DIR) { $env:AZURE_CONFIG_DIR } else { (Join-Path (Split-Path $PSScriptRoot -Parent) ".azure-cli") })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path $PSScriptRoot -Parent
$FunctionProjectPath = Join-Path $RepoRoot "function_apps\adf_tickers_ingest"
$FunctionLinkedServicePath = Join-Path $RepoRoot "workflows\adf_linked_services_function_tickers.json"
$FunctionPipelinePath = Join-Path $RepoRoot "workflows\adf_pipeline_function_tickers.json"
$AzureCliExe = $null
$AzureCliResolved = $false
$UsePythonAzCli = $false
$FunctionAppUrl = $null
$FunctionKey = $null
$FunctionIdentityPrincipalId = $null
$DataStorageId = $null
$PublishingAuthHeaders = $null
$SecUserAgent = $null

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

function Enable-Tls12 {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
}

function Clear-BrokenLoopbackProxy {
    foreach ($proxyVar in @("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")) {
        $value = [Environment]::GetEnvironmentVariable($proxyVar)
        if ([string]::IsNullOrWhiteSpace($value)) {
            continue
        }
        if ($value -match '^http://(127\.0\.0\.1|localhost):9/?$') {
            [Environment]::SetEnvironmentVariable($proxyVar, $null)
            Set-Item -Path ("Env:" + $proxyVar) -Value $null -ErrorAction SilentlyContinue
        }
    }
}

function Resolve-AzRunner {
    if ($script:AzureCliResolved) {
        return
    }

    $azCommand = Get-Command az -ErrorAction Stop
    $script:AzureCliExe = $azCommand.Source

    if ($env:OS -eq "Windows_NT") {
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

    Invoke-Az -Args @("config", "set", "extension.dynamic_install_allow_preview=true", "--output", "none") | Out-Null
    Invoke-Az -Args @("extension", "add", "--name", $Name, "--yes", "--output", "none") | Out-Null
}

function Ensure-ProviderRegistration([string]$Namespace) {
    $state = Invoke-Az -Args @(
        "provider", "show",
        "--namespace", $Namespace,
        "--query", "registrationState",
        "--output", "tsv"
    ) -AllowFailure

    if ($state.Success -and $state.Output -eq "Registered") {
        Write-Status "Provider $Namespace already registered" "SKIPPED" DarkGray
        return
    }

    Write-Host "  Registering provider $Namespace..." -NoNewline
    Invoke-Az -Args @(
        "provider", "register",
        "--namespace", $Namespace,
        "--output", "none"
    ) | Out-Null

    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $status = Invoke-Az -Args @(
            "provider", "show",
            "--namespace", $Namespace,
            "--query", "registrationState",
            "--output", "tsv"
        ) -AllowFailure
        if ($status.Success -and $status.Output -eq "Registered") {
            Write-Host "  [OK]" -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 5
    }

    throw "Provider namespace '$Namespace' did not reach Registered state in time."
}

function Ensure-AzureContext {
    Write-Section "[1/7] Verifying Azure CLI context..."
    Ensure-AzConfigDir
    Enable-Tls12
    Clear-BrokenLoopbackProxy
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

    $suffix = ([string]$account.id).Replace("-", "").Substring(0, 8).ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($FunctionAppName)) {
        $script:FunctionAppName = "sec-edgar-flex-$suffix"
    }
    if ([string]::IsNullOrWhiteSpace($FunctionStorageAccount)) {
        $script:FunctionStorageAccount = "secedgarfn$suffix"
    }

    if (-not [string]::IsNullOrWhiteSpace($env:SEC_USER_AGENT)) {
        $script:SecUserAgent = $env:SEC_USER_AGENT.Trim()
    } elseif ([string]$account.user -match "@") {
        $script:SecUserAgent = "SEC EDGAR Bronze Pipeline $($account.user)"
    } else {
        throw "SEC_USER_AGENT is required. Set SEC_USER_AGENT or sign in with an Azure account that exposes a contact email."
    }

    Write-Status "Subscription: $($account.name) ($($account.id))" "OK" Green
    Write-Status "Function App: $FunctionAppName" "OK" Green
    Write-Status "Function host storage: $FunctionStorageAccount" "OK" Green

    Ensure-ProviderRegistration "Microsoft.Web"
    Ensure-ProviderRegistration "Microsoft.Insights"
    Ensure-ProviderRegistration "Microsoft.Storage"
}

function Ensure-StorageAccount {
    Write-Section "[2/7] Ensuring Function host storage..."
    $existing = Invoke-Az -Args @(
        "storage", "account", "show",
        "--name", $FunctionStorageAccount,
        "--resource-group", $ResourceGroup,
        "--query", "{name:name,isHnsEnabled:isHnsEnabled,kind:kind}",
        "--output", "json"
    ) -AllowFailure

    if (-not $existing.Success) {
        Invoke-Az -Args @(
            "storage", "account", "create",
            "--name", $FunctionStorageAccount,
            "--resource-group", $ResourceGroup,
            "--location", $Location,
            "--sku", "Standard_LRS",
            "--kind", "StorageV2",
            "--https-only", "true",
            "--allow-blob-public-access", "false",
            "--output", "none"
        ) | Out-Null
        Write-Status "Created Function host storage account" "OK" Green
        return
    }

    $storage = $existing.Output | ConvertFrom-Json
    if ($storage.isHnsEnabled) {
        throw "Function host storage account '$FunctionStorageAccount' has Hierarchical Namespace enabled. Azure Functions host storage must be a regular StorageV2 account for this implementation."
    }
    Write-Status "Function host storage account already exists" "SKIPPED" DarkGray
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

function New-FunctionPackageZip {
    $bundleStage = Join-Path $env:TEMP ("sec-edgar-function-tickers-" + [guid]::NewGuid().ToString("N"))
    $bundleZip = Join-Path $env:TEMP ("sec-edgar-function-tickers-" + [guid]::NewGuid().ToString("N") + ".zip")
    New-Item -ItemType Directory -Path $bundleStage | Out-Null

    try {
        Copy-Item -Path (Join-Path $FunctionProjectPath ".deployment") -Destination (Join-Path $bundleStage ".deployment")
        Copy-Item -Path (Join-Path $FunctionProjectPath "host.json") -Destination (Join-Path $bundleStage "host.json")
        Copy-Item -Path (Join-Path $FunctionProjectPath "requirements.txt") -Destination (Join-Path $bundleStage "requirements.txt")
        Copy-Item -Path (Join-Path $FunctionProjectPath "ingest_tickers_exchange") -Destination (Join-Path $bundleStage "ingest_tickers_exchange") -Recurse
        Copy-Item -Path (Join-Path $RepoRoot "config") -Destination (Join-Path $bundleStage "config") -Recurse

        $scriptsStage = Join-Path $bundleStage "scripts"
        New-Item -ItemType Directory -Path $scriptsStage | Out-Null
        Copy-Item -Path (Join-Path $RepoRoot "scripts\__init__.py") -Destination (Join-Path $scriptsStage "__init__.py")
        Copy-Item -Path (Join-Path $RepoRoot "scripts\ingest") -Destination (Join-Path $scriptsStage "ingest") -Recurse

        Get-ChildItem -Path $bundleStage -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
        Get-ChildItem -Path $bundleStage -Recurse -File -Include "*.pyc" | Remove-Item -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath (Join-Path $scriptsStage "ingest\test_sec_loader.py") -Force -ErrorAction SilentlyContinue

        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zipArchive = [System.IO.Compression.ZipFile]::Open($bundleZip, [System.IO.Compression.ZipArchiveMode]::Create)
        try {
            $files = Get-ChildItem -Path $bundleStage -Recurse -File
            foreach ($file in $files) {
                $relativePath = $file.FullName.Substring($bundleStage.Length).TrimStart('\')
                $entryName = $relativePath -replace '\\', '/'
                [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                    $zipArchive,
                    $file.FullName,
                    $entryName,
                    [System.IO.Compression.CompressionLevel]::Optimal
                ) | Out-Null
            }
        } finally {
            $zipArchive.Dispose()
        }

        return $bundleZip
    } finally {
        Remove-Item -LiteralPath $bundleStage -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Assert-FunctionBuildSettings {
    $settings = Invoke-Az -Args @(
        "functionapp", "config", "appsettings", "list",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--query", "[?name=='SEC_USER_AGENT' || name=='CLOUD_PROVIDER' || name=='AZURE_STORAGE_ACCOUNT' || name=='AZURE_CONTAINER' || name=='STORAGE_PREFIX'].{name:name,value:value}",
        "--output", "json"
    ) -Json

    $settingsByName = @{}
    foreach ($setting in @($settings)) {
        $settingsByName[[string]$setting.name] = [string]$setting.value
    }

    if ([string]::IsNullOrWhiteSpace($settingsByName["SEC_USER_AGENT"])) {
        throw "SEC_USER_AGENT was not applied to the Function App."
    }
    if ($settingsByName["CLOUD_PROVIDER"] -ne "azure") {
        throw "CLOUD_PROVIDER=azure was not applied to the Function App."
    }
    if ($settingsByName["AZURE_STORAGE_ACCOUNT"] -ne $DataStorageAccount) {
        throw "AZURE_STORAGE_ACCOUNT was not applied to the Function App."
    }
}

function Get-FunctionDiagnostics {
    $diagnostics = [System.Collections.Generic.List[string]]::new()

    $deploymentLogs = Invoke-Az -Args @(
        "functionapp", "log", "deployment", "show",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--output", "json"
    ) -AllowFailure
    if ($deploymentLogs.Success -and -not [string]::IsNullOrWhiteSpace($deploymentLogs.Output) -and $deploymentLogs.Output -ne "[]") {
        [void]$diagnostics.Add("Deployment log:")
        [void]$diagnostics.Add($deploymentLogs.Output)
    }

    $hostLogs = Invoke-Az -Args @(
        "monitor", "app-insights", "query",
        "--app", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--analytics-query", "union traces, exceptions | where timestamp > ago(30m) | project timestamp, itemType, message, outerMessage, problemId | order by timestamp desc | take 20",
        "--output", "json"
    ) -AllowFailure
    if ($hostLogs.Success -and -not [string]::IsNullOrWhiteSpace($hostLogs.Output)) {
        [void]$diagnostics.Add("Application Insights:")
        [void]$diagnostics.Add($hostLogs.Output)
    }

    $contentShare = Invoke-Az -Args @(
        "functionapp", "config", "appsettings", "list",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--query", "[?name=='WEBSITE_CONTENTSHARE'].value | [0]",
        "--output", "tsv"
    ) -AllowFailure
    if ($contentShare.Success -and -not [string]::IsNullOrWhiteSpace($contentShare.Output)) {
        $functionStorageKey = Invoke-Az -Args @(
            "storage", "account", "keys", "list",
            "--account-name", $FunctionStorageAccount,
            "--resource-group", $ResourceGroup,
            "--query", "[0].value",
            "--output", "tsv"
        ) -AllowFailure
        if ($functionStorageKey.Success -and -not [string]::IsNullOrWhiteSpace($functionStorageKey.Output)) {
            $shareListing = Invoke-Az -Args @(
                "storage", "directory", "list",
                "--share-name", $contentShare.Output,
                "--name", "site/wwwroot",
                "--account-name", $FunctionStorageAccount,
                "--account-key", $functionStorageKey.Output,
                "--output", "json"
            ) -AllowFailure
            if ($shareListing.Success) {
                [void]$diagnostics.Add("Content share listing:")
                [void]$diagnostics.Add($shareListing.Output)
            }
        }
    }

    return ($diagnostics -join "`n")
}

function Ensure-FunctionApp {
    Write-Section "[3/7] Ensuring Azure Function App..."
    $script:DataStorageId = Invoke-Az -Args @(
        "storage", "account", "show",
        "--name", $DataStorageAccount,
        "--resource-group", $ResourceGroup,
        "--query", "id",
        "--output", "tsv"
    )

    $existing = Invoke-Az -Args @(
        "functionapp", "show",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--query", "{name:name,kind:kind}",
        "--output", "json"
    ) -AllowFailure

    if (-not $existing.Success) {
        Invoke-Az -Args @(
            "functionapp", "create",
            "--name", $FunctionAppName,
            "--resource-group", $ResourceGroup,
            "--storage-account", $FunctionStorageAccount,
            "--flexconsumption-location", $Location,
            "--functions-version", "4",
            "--runtime", "python",
            "--runtime-version", "3.11",
            "--instance-memory", "2048",
            "--assign-identity", "[system]",
            "--role", "Storage Blob Data Contributor",
            "--scope", $DataStorageId,
            "--output", "none"
        ) | Out-Null
        Write-Status "Created Azure Function App" "OK" Green
    } else {
        Write-Status "Azure Function App already exists" "SKIPPED" DarkGray
    }

    Invoke-Az -Args @(
        "functionapp", "identity", "assign",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--output", "none"
    ) | Out-Null

    $identity = Invoke-Az -Args @(
        "functionapp", "identity", "show",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--query", "{principalId:principalId}",
        "--output", "json"
    ) -Json
    $script:FunctionIdentityPrincipalId = [string]$identity.principalId

    Write-Section "[4/7] Configuring Function identity and app settings..."
    Ensure-RoleAssignment $FunctionIdentityPrincipalId $DataStorageId "Storage Blob Data Contributor" "Function MI -> Data Lake (Blob Data Contributor)"

    Invoke-Az -Args @(
        "functionapp", "config", "appsettings", "set",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--settings",
        "SEC_USER_AGENT=$SecUserAgent",
        "CLOUD_PROVIDER=azure",
        "AZURE_STORAGE_ACCOUNT=$DataStorageAccount",
        "AZURE_CONTAINER=$Container",
        "STORAGE_PREFIX=$Prefix",
        "--output", "none"
    ) | Out-Null

    Assert-FunctionBuildSettings
}

function Deploy-FunctionCode {
    Write-Section "[5/7] Deploying Function code..."
    $bundleZip = New-FunctionPackageZip
    try {
        Invoke-Az -Args @(
            "functionapp", "deployment", "source", "config-zip",
            "--name", $FunctionAppName,
            "--resource-group", $ResourceGroup,
            "--src", $bundleZip,
            "--build-remote", "true",
            "--output", "none"
        ) | Out-Null
    } finally {
        Remove-Item -LiteralPath $bundleZip -Force -ErrorAction SilentlyContinue
    }

    Invoke-Az -Args @(
        "functionapp", "restart",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--output", "none"
    ) | Out-Null

    $functions = $null
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $functionsResponse = Invoke-Az -Args @(
            "functionapp", "function", "list",
            "--name", $FunctionAppName,
            "--resource-group", $ResourceGroup,
            "--output", "json"
        ) -AllowFailure
        if ($functionsResponse.Success -and -not [string]::IsNullOrWhiteSpace($functionsResponse.Output)) {
            $functions = $functionsResponse.Output | ConvertFrom-Json
        } else {
            $functions = $null
        }
        if ($functions -and (@($functions | Where-Object { $_.name -like "*ingest_tickers_exchange" }).Count -gt 0)) {
            break
        }
        Start-Sleep -Seconds 10
    }

    if (-not $functions -or (@($functions | Where-Object { $_.name -like "*ingest_tickers_exchange" }).Count -eq 0)) {
        $diagnostics = Get-FunctionDiagnostics
        throw "Function app deployment completed, but 'ingest_tickers_exchange' was not indexed by the Functions runtime.`n$diagnostics"
    }

    $defaultHostName = Invoke-Az -Args @(
        "functionapp", "show",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--query", "properties.defaultHostName",
        "--output", "tsv"
    )
    $script:FunctionAppUrl = "https://$defaultHostName"
    $script:FunctionKey = Invoke-Az -Args @(
        "functionapp", "keys", "list",
        "--name", $FunctionAppName,
        "--resource-group", $ResourceGroup,
        "--query", "functionKeys.default",
        "--output", "tsv"
    )

    Write-Status "Function deployed: $FunctionAppUrl" "OK" Green
}

function Invoke-FunctionDirectly {
    Write-Section "[6/7] Invoking Function directly..."
    $uri = "$FunctionAppUrl/api/ingest_tickers_exchange?code=$FunctionKey"
    $bodyFile = [System.IO.Path]::GetTempFileName() + ".json"
    $lastError = $null

    try {
        @{ ingestDate = $IngestDate } | ConvertTo-Json -Compress | Set-Content -Path $bodyFile -Encoding UTF8

        for ($attempt = 0; $attempt -lt 12; $attempt++) {
            try {
                $responseText = Invoke-Az -Args @(
                    "rest",
                    "--method", "post",
                    "--url", $uri,
                    "--skip-authorization-header",
                    "--headers", "Content-Type=application/json",
                    "--body", "@$bodyFile",
                    "--output", "json"
                )
                $response = $responseText | ConvertFrom-Json
                if ($response.status -ne "Succeeded") {
                    throw "Function returned status '$($response.status)'"
                }
                Write-Status "Function invocation succeeded for ingestDate=$IngestDate" "OK" Green
                return
            } catch {
                $lastError = $_
                Start-Sleep -Seconds 10
            }
        }
    } finally {
        Remove-Item -LiteralPath $bodyFile -Force -ErrorAction SilentlyContinue
    }

    throw "Direct Function invocation failed after retries: $($lastError.Exception.Message)"
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
        Remove-Item -LiteralPath $tmpFile -Force -ErrorAction SilentlyContinue
    }
}

function Ensure-AdfArtifacts {
    Write-Section "[7/7] Creating ADF Azure Function artifacts and validating output..."
    $linkedServiceDefinition = Get-Content $FunctionLinkedServicePath -Raw | ConvertFrom-Json
    foreach ($linkedService in $linkedServiceDefinition.linkedServices) {
        $linkedService.name = $FunctionLinkedServiceName
        $linkedService.properties.typeProperties.functionAppUrl = $FunctionAppUrl
        $linkedService.properties.typeProperties.functionKey = [pscustomobject]@{
            type = "SecureString"
            value = $FunctionKey
        }
        Write-Host "  Linked service: $($linkedService.name)" -NoNewline
        Invoke-AdfCreate "linked-service" $linkedService.name "--properties" $linkedService.properties
        Write-Host "  [OK]" -ForegroundColor Green
    }

    $pipelineDefinition = Get-Content $FunctionPipelinePath -Raw | ConvertFrom-Json
    $pipelineDefinition.name = $PipelineName
    $pipelineDefinition.properties.parameters.ingestDate.defaultValue = $IngestDate
    $pipelineDefinition.properties.activities[0].linkedServiceName.referenceName = $FunctionLinkedServiceName
    Write-Host "  Pipeline: $PipelineName" -NoNewline
    Invoke-AdfCreate "pipeline" $PipelineName "--pipeline" $pipelineDefinition.properties
    Write-Host "  [OK]" -ForegroundColor Green

    $paramsFile = [System.IO.Path]::GetTempFileName() + ".json"
    try {
        @{ ingestDate = $IngestDate } | ConvertTo-Json -Compress | Set-Content -Path $paramsFile -Encoding UTF8
        $runId = Invoke-Az -Args @(
            "datafactory", "pipeline", "create-run",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--name", $PipelineName,
            "--parameters", "@$paramsFile",
            "--query", "runId",
            "--output", "tsv"
        )
    } finally {
        Remove-Item -LiteralPath $paramsFile -Force -ErrorAction SilentlyContinue
    }

    $status = $null
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $status = Invoke-Az -Args @(
            "datafactory", "pipeline-run", "show",
            "--factory-name", $AdfName,
            "--resource-group", $ResourceGroup,
            "--run-id", $runId,
            "--query", "status",
            "--output", "tsv"
        )
        if ($status -in @("Succeeded", "Failed", "Cancelled")) {
            break
        }
        Start-Sleep -Seconds 10
    }

    if ($status -ne "Succeeded") {
        throw "ADF Function pipeline run '$runId' ended with status '$status'"
    }

    $outputPath = "$Prefix/bronze/company_tickers_exchange/ingestion_date=$IngestDate/data.parquet"
    $outputFile = Invoke-Az -Args @(
        "storage", "fs", "file", "show",
        "--file-system", $Container,
        "--path", $outputPath,
        "--account-name", $DataStorageAccount,
        "--auth-mode", "login",
        "--output", "json"
    ) -AllowFailure

    if (-not $outputFile.Success) {
        throw "Ticker Parquet output not found at '$Container/$outputPath'"
    }

    Write-Status "ADF Function pipeline succeeded" "OK" Green
    Write-Status "Ticker Parquet exists at $Container/$outputPath" "OK" Green
}

Ensure-AzureContext
Ensure-StorageAccount
Ensure-FunctionApp
Deploy-FunctionCode
Invoke-FunctionDirectly
Ensure-AdfArtifacts

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  AZURE FUNCTION TICKER INGEST COMPLETE" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
Write-Host "  Function App  : $FunctionAppName"
Write-Host "  Function URL  : $FunctionAppUrl/api/ingest_tickers_exchange"
Write-Host "  ADF Pipeline  : $PipelineName"
Write-Host "  Ingest Date   : $IngestDate"
Write-Host "=================================================================" -ForegroundColor Green
