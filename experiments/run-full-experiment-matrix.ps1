[CmdletBinding()]
param(
    [string]$MatrixName = ("experiment-matrix-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")),
    [string]$ResultsRoot = "results",
    [ValidateSet("Smoke", "Dissertation")][string]$Preset = "Smoke",
    [string[]]$Scenarios = @(),
    [string]$ScenarioDirectory = "",
    [string[]]$Approaches = @("static", "reactive", "adaptive"),
    [ValidateRange(0, 20)][int]$Replications = 0,
    [string[]]$StaticConsumerCounts = @(),
    [int]$RandomSeed = 20260917,
    [int]$ConsumerCpuIterations = 1000,
    [int]$ConsumerProcessingDelayMs = 5,
    [int]$SampleIntervalSeconds = 2,
    [int]$DrainTimeoutSeconds = 45,
    [string]$ConsumerContainerCpus = "1.0",
    [string]$ConsumerContainerMemory = "512m",
    [double]$InitialCapacityPerConsumer = 60.0,
    [double]$CapacityHeadroom = 1.20,
    [int]$CooldownSeconds = 8,
    [switch]$Resume,
    [switch]$ScheduleOnly,
    [switch]$SkipKafkaStart,
    [switch]$SkipBuild,
    [switch]$SkipImageBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-UtcTimestamp { return (Get-Date).ToUniversalTime().ToString("o") }

function Convert-ToList {
    param([string[]]$Values)
    $items = @()
    foreach ($value in $Values) {
        foreach ($part in ($value -split ",")) {
            $trimmed = $part.Trim()
            if (-not [string]::IsNullOrWhiteSpace($trimmed)) { $items += $trimmed }
        }
    }
    return @($items | Select-Object -Unique)
}

function Invoke-Runner {
    param([string]$Script, [string[]]$Arguments)
    & powershell -NoProfile -ExecutionPolicy Bypass -File $Script @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Experiment runner failed with exit code ${LASTEXITCODE}: $Script" }
}

function Read-SummaryRecord {
    param([object]$Entry, [string]$Path)
    $summary = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    $producer = $summary.producerSummary
    $aggregate = $summary.aggregateConsumerMetrics
    $finalLag = if ($null -ne $summary.finalKafkaLag) { $summary.finalKafkaLag.lag } else { $null }
    $evidenceReady = ($summary.status -eq "completed" -and $producer.acknowledged -eq $aggregate.processed -and $aggregate.failed -eq 0 -and $finalLag -eq 0)
    return [pscustomobject]@{
        scheduleId = $Entry.id; sequence = $Entry.sequence; attempt = $Entry.attempts
        approach = $Entry.approachLabel; scenario = $Entry.scenario; replication = $Entry.replication
        status = $summary.status; producerAcknowledged = $producer.acknowledged
        producerRatePerSecond = $producer.acknowledgedRatePerSecond; consumerProcessed = $aggregate.processed
        consumerFailed = $aggregate.failed; peakLag = $summary.peakKafkaLag; finalLag = $finalLag
        maxObservedConsumers = $summary.maxObservedConsumers; evidenceReady = $evidenceReady; summary = $Path
    }
}

function Save-Schedule {
    $script:schedule | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath $script:schedulePath -Encoding UTF8
}

function Write-MatrixManifest {
    $records = @($script:schedule.entries | Where-Object { $null -ne $_.result } | ForEach-Object { $_.result })
    $remaining = @($script:schedule.entries | Where-Object { $_.status -ne "completed" }).Count
    $manifest = [pscustomobject]@{
        schemaVersion = 2; matrixName = $script:schedule.matrixName; preset = $script:schedule.preset
        startedAt = $script:schedule.createdAt; updatedAt = New-UtcTimestamp
        completedAt = if ($remaining -eq 0) { New-UtcTimestamp } else { $null }
        randomSeed = $script:schedule.randomSeed; schedule = $script:schedulePath
        scenarios = $script:schedule.scenarios; scenarioDirectory = $script:schedule.scenarioDirectory
        approaches = $script:schedule.approaches; executionSubstrate = "docker"
        consumerContainerLimits = $script:schedule.consumerContainerLimits
        replications = $script:schedule.replications; staticConsumerCounts = $script:schedule.staticConsumerCounts
        experimentCount = @($script:schedule.entries).Count
        completedCount = @($script:schedule.entries | Where-Object { $_.status -eq "completed" }).Count
        failedCount = @($script:schedule.entries | Where-Object { $_.status -eq "failed" }).Count
        pendingCount = @($script:schedule.entries | Where-Object { $_.status -in @("pending", "running") }).Count
        evidenceReadyCount = @($records | Where-Object { $_.evidenceReady }).Count
        evidenceReady = ($records.Count -eq @($script:schedule.entries).Count -and @($records | Where-Object { -not $_.evidenceReady }).Count -eq 0)
        runs = $records
    }
    $manifest | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath $script:manifestPath -Encoding UTF8
}

function New-RandomizedEntries {
    param([string[]]$ScenarioNames, [string[]]$ApproachNames, [string[]]$ConsumerCounts, [int]$ReplicationCount, [int]$Seed)
    $entries = [System.Collections.ArrayList]::new()
    foreach ($scenario in $ScenarioNames) {
        if ("static" -in $ApproachNames) {
            foreach ($count in $ConsumerCounts) {
                for ($replication = 1; $replication -le $ReplicationCount; $replication++) {
                    [void]$entries.Add([pscustomobject]@{
                        id = "$scenario-static-c$count-r$replication"; sequence = 0; scenario = $scenario
                        policy = "Static"; approachLabel = "static-c$count"; fixedConsumers = [int]$count
                        replication = $replication; status = "pending"; attempts = 0
                        startedAt = $null; completedAt = $null; error = $null; result = $null
                    })
                }
            }
        }
        foreach ($approach in @("reactive", "adaptive")) {
            if ($approach -notin $ApproachNames) { continue }
            for ($replication = 1; $replication -le $ReplicationCount; $replication++) {
                [void]$entries.Add([pscustomobject]@{
                    id = "$scenario-$approach-r$replication"; sequence = 0; scenario = $scenario
                    policy = (Get-Culture).TextInfo.ToTitleCase($approach); approachLabel = $approach; fixedConsumers = $null
                    replication = $replication; status = "pending"; attempts = 0
                    startedAt = $null; completedAt = $null; error = $null; result = $null
                })
            }
        }
    }
    $random = [System.Random]::new($Seed)
    for ($index = $entries.Count - 1; $index -gt 0; $index--) {
        $swapIndex = $random.Next($index + 1)
        $temporary = $entries[$index]; $entries[$index] = $entries[$swapIndex]; $entries[$swapIndex] = $temporary
    }
    for ($index = 0; $index -lt $entries.Count; $index++) { $entries[$index].sequence = $index + 1 }
    return @($entries)
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$matrixRoot = Join-Path (Join-Path $repoRoot $ResultsRoot) $MatrixName
$script:schedulePath = Join-Path $matrixRoot "execution-schedule.json"
$script:manifestPath = Join-Path $matrixRoot "matrix-manifest.json"

if ($Resume) {
    if (-not (Test-Path -LiteralPath $script:schedulePath)) { throw "Cannot resume: schedule not found at $script:schedulePath" }
    $script:schedule = Get-Content -LiteralPath $script:schedulePath -Raw | ConvertFrom-Json
    if ($script:schedule.randomSeed -ne $RandomSeed) { throw "Resume seed mismatch. Schedule uses $($script:schedule.randomSeed), command supplied $RandomSeed." }
    foreach ($entry in $script:schedule.entries) {
        if ($entry.status -eq "running") { $entry.status = "failed"; $entry.error = "Previous process ended while this run was active." }
    }
    $savedSettings = $script:schedule.PSObject.Properties["executionSettings"]
    if ($null -ne $savedSettings) {
        $ConsumerCpuIterations = $savedSettings.Value.consumerCpuIterations
        $ConsumerProcessingDelayMs = $savedSettings.Value.consumerProcessingDelayMs
        $SampleIntervalSeconds = $savedSettings.Value.sampleIntervalSeconds
        $DrainTimeoutSeconds = $savedSettings.Value.drainTimeoutSeconds
        $InitialCapacityPerConsumer = $savedSettings.Value.initialCapacityPerConsumer
        $CapacityHeadroom = $savedSettings.Value.capacityHeadroom
        $CooldownSeconds = $savedSettings.Value.cooldownSeconds
    }
    Save-Schedule
} else {
    if (Test-Path -LiteralPath $matrixRoot) { throw "Matrix directory already exists: $matrixRoot. Choose a new name or use -Resume." }
    $selectedScenarios = @(Convert-ToList -Values $Scenarios)
    $selectedApproaches = @(Convert-ToList -Values $Approaches | ForEach-Object { $_.ToLowerInvariant() })
    $consumerCounts = @(Convert-ToList -Values $StaticConsumerCounts)
    if ($selectedScenarios.Count -eq 0) { $selectedScenarios = if ($Preset -eq "Smoke") { @("steady") } else { @("steady-small", "steady-large", "ramp-small", "burst-quiet-medium") } }
    if ($Replications -eq 0) { $Replications = if ($Preset -eq "Smoke") { 1 } else { 3 } }
    if ($consumerCounts.Count -eq 0) { $consumerCounts = if ($Preset -eq "Smoke") { @("1") } else { @("1", "2", "4") } }
    $resolvedScenarioDirectory = if ([string]::IsNullOrWhiteSpace($ScenarioDirectory)) { if ($Preset -eq "Smoke") { "producer/scenarios" } else { "experiments/scenarios" } } else { $ScenarioDirectory.TrimEnd([char[]]@('/', '\')) }
    $allowedApproaches = @("static", "reactive", "adaptive")
    foreach ($approach in $selectedApproaches) { if ($approach -notin $allowedApproaches) { throw "Unknown approach '$approach'." } }
    foreach ($scenario in $selectedScenarios) {
        $scenarioPath = Join-Path $repoRoot "$resolvedScenarioDirectory/$scenario.properties"
        if (-not (Test-Path -LiteralPath $scenarioPath)) { throw "Scenario file not found: $scenarioPath" }
    }
    New-Item -ItemType Directory -Force -Path $matrixRoot | Out-Null
    $script:schedule = [pscustomobject]@{
        schemaVersion = 1; matrixName = $MatrixName; preset = $Preset; createdAt = New-UtcTimestamp
        randomSeed = $RandomSeed; scenarios = $selectedScenarios; scenarioDirectory = $resolvedScenarioDirectory
        approaches = $selectedApproaches; replications = $Replications; staticConsumerCounts = $consumerCounts
        consumerContainerLimits = [pscustomobject]@{ cpus = $ConsumerContainerCpus; memory = $ConsumerContainerMemory }
        executionSettings = [pscustomobject]@{
            consumerCpuIterations = $ConsumerCpuIterations; consumerProcessingDelayMs = $ConsumerProcessingDelayMs
            sampleIntervalSeconds = $SampleIntervalSeconds; drainTimeoutSeconds = $DrainTimeoutSeconds
            initialCapacityPerConsumer = $InitialCapacityPerConsumer; capacityHeadroom = $CapacityHeadroom
            cooldownSeconds = $CooldownSeconds
        }
        entries = @(New-RandomizedEntries -ScenarioNames $selectedScenarios -ApproachNames $selectedApproaches -ConsumerCounts $consumerCounts -ReplicationCount $Replications -Seed $RandomSeed)
    }
    Save-Schedule
    Write-MatrixManifest
}

if ($ScheduleOnly) {
    Write-Output "MATRIX_SCHEDULE $script:schedulePath"
    Write-Output "MATRIX_MANIFEST $script:manifestPath"
    exit 0
}

Push-Location -LiteralPath $repoRoot
try {
    if (-not $SkipBuild) {
        & mvn -B -ntp -f producer/pom.xml package; if ($LASTEXITCODE -ne 0) { throw "Producer package failed." }
        & mvn -B -ntp -f consumer/pom.xml package; if ($LASTEXITCODE -ne 0) { throw "Consumer package failed." }
    }
    if (-not $SkipKafkaStart) {
        & docker-compose -f docker/docker-compose.yml up -d --wait kafka; if ($LASTEXITCODE -ne 0) { throw "Kafka startup failed." }
        & docker-compose -f docker/docker-compose.yml run --rm kafka-init; if ($LASTEXITCODE -ne 0) { throw "Kafka topic initialization failed." }
    }
    if (-not $SkipImageBuild) { & docker build -t dissertation-telecom-consumer:0.1.0 consumer; if ($LASTEXITCODE -ne 0) { throw "Consumer image build failed." } }

    foreach ($entry in @($script:schedule.entries | Sort-Object sequence)) {
        if ($entry.status -eq "completed") { continue }
        $entry.attempts = [int]$entry.attempts + 1
        $entry.status = "running"; $entry.startedAt = New-UtcTimestamp; $entry.completedAt = $null; $entry.error = $null
        Save-Schedule; Write-MatrixManifest
        $runName = "$($entry.id)-a$($entry.attempts)"
        $scenarioResultsRoot = Join-Path (Join-Path $ResultsRoot $MatrixName) $entry.scenario
        $scenarioFile = "$($script:schedule.scenarioDirectory)/$($entry.scenario).properties"
        $common = @(
            "-ControllerMode", $entry.policy, "-RunName", $runName, "-ResultsRoot", $scenarioResultsRoot,
            "-ConsumerGroup", "$MatrixName-$($entry.id)-a$($entry.attempts)", "-MinConsumers", "1", "-MaxConsumers", "4",
            "-SampleIntervalSeconds", $SampleIntervalSeconds, "-DrainTimeoutSeconds", $DrainTimeoutSeconds,
            "-ConsumerCpuIterations", $ConsumerCpuIterations, "-ConsumerProcessingDelayMs", $ConsumerProcessingDelayMs,
            "-ConsumerContainerCpus", $script:schedule.consumerContainerLimits.cpus,
            "-ConsumerContainerMemory", $script:schedule.consumerContainerLimits.memory,
            "-ProducerScenarioFile", $scenarioFile, "-SkipBuild", "-SkipKafkaStart", "-SkipImageBuild"
        )
        if ($entry.policy -eq "Static") { $runnerArgs = $common + @("-FixedConsumers", $entry.fixedConsumers) }
        elseif ($entry.policy -eq "Reactive") { $runnerArgs = $common + @("-ScaleUpLagThreshold", "20", "-ScaleDownLagThreshold", "0", "-CooldownSeconds", $CooldownSeconds) }
        else { $runnerArgs = $common + @("-ScaleUpLagThreshold", "5", "-ScaleDownLagThreshold", "0", "-InitialCapacityPerConsumer", $InitialCapacityPerConsumer, "-CapacityHeadroom", $CapacityHeadroom, "-CooldownSeconds", $CooldownSeconds) }
        try {
            Invoke-Runner -Script (Join-Path $PSScriptRoot "run-docker-scaling.ps1") -Arguments $runnerArgs
            $summaryPath = Join-Path $matrixRoot "$($entry.scenario)/$runName/docker-scaling-summary.json"
            $entry.result = Read-SummaryRecord -Entry $entry -Path $summaryPath
            $entry.status = "completed"; $entry.completedAt = New-UtcTimestamp
            Save-Schedule; Write-MatrixManifest
        } catch {
            $entry.status = "failed"; $entry.completedAt = New-UtcTimestamp; $entry.error = $_.Exception.Message
            Save-Schedule; Write-MatrixManifest
            throw
        }
    }
    Write-Output "MATRIX_SCHEDULE $script:schedulePath"
    Write-Output "MATRIX_MANIFEST $script:manifestPath"
    Write-Output ((Get-Content -LiteralPath $script:manifestPath -Raw | ConvertFrom-Json) | ConvertTo-Json -Compress -Depth 60)
} finally {
    Pop-Location
}
