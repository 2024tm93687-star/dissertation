[CmdletBinding()]
param(
    [string]$RunName = ("docker-scaling-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")),
    [string]$ResultsRoot = "results",
    [ValidateSet("Static", "Reactive", "Adaptive")]
    [string]$ControllerMode = "Reactive",
    [string]$Topic = "telecom-events",
    [ValidateRange(1, 1000)]
    [int]$TopicPartitions = 6,
    [string]$ConsumerGroup = ("docker-scale-" + [guid]::NewGuid().ToString("N")),
    [ValidateRange(1, 32)]
    [int]$MinConsumers = 1,
    [ValidateRange(1, 32)]
    [int]$MaxConsumers = 2,
    [int]$ScaleUpLagThreshold = 5,
    [int]$ScaleDownLagThreshold = 0,
    [ValidateRange(1, 32)]
    [int]$FixedConsumers = 1,
    [double]$InitialCapacityPerConsumer = 60.0,
    [double]$CapacityHeadroom = 1.20,
    [double]$TargetLatencyP95Millis = 2000.0,
    [int]$CooldownSeconds = 5,
    [int]$SampleIntervalSeconds = 2,
    [int]$DrainTimeoutSeconds = 90,
    [int]$ConsumerCpuIterations = 1000,
    [int]$ConsumerProcessingDelayMs = 0,
    [int]$ConsumerReportIntervalMs = 2000,
    [int]$ConsumerMaxPollRecords = 500,
    [int]$ConsumerFetchMinBytes = 1,
    [int]$ConsumerMaxPartitionFetchBytes = 1048576,
    [string]$ConsumerContainerCpus = "1.0",
    [string]$ConsumerContainerMemory = "512m",
    [string]$ConsumerImage = "dissertation-telecom-consumer:0.1.0",
    [string]$ProducerScenarioFile = "",
    [int]$ProducerEventCount = 60,
    [int]$ProducerRatePerSecond = 30,
    [int]$ProducerPayloadSizeBytes = 256,
    [switch]$SkipKafkaStart,
    [switch]$SkipBuild,
    [switch]$SkipImageBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-UtcTimestamp { return (Get-Date).ToUniversalTime().ToString("o") }

function Convert-ToPowerShellLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Write-JavaWrapper {
    param([string]$Path, [string]$ExitCodePath, [string[]]$Arguments)
    $lines = @('$ErrorActionPreference = "Continue"', '$javaArgs = @(')
    foreach ($argument in $Arguments) { $lines += "    $(Convert-ToPowerShellLiteral $argument)" }
    $lines += ')'
    $lines += '& java @javaArgs'
    $lines += '$exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }'
    $lines += "Set-Content -LiteralPath $(Convert-ToPowerShellLiteral $ExitCodePath) -Value `$exitCode -Encoding ASCII"
    $lines += 'exit $exitCode'
    Set-Content -LiteralPath $Path -Value $lines -Encoding UTF8
}

function Read-ExitCode {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $code = 0
    if ([int]::TryParse((Get-Content -LiteralPath $Path -TotalCount 1).Trim(), [ref]$code)) { return $code }
    return $null
}

function Read-PrefixedJson {
    param([string]$Path, [string]$Prefix)
    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) { return $items }
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line.StartsWith($Prefix)) {
            try { $items += ($line.Substring($Prefix.Length).Trim() | ConvertFrom-Json) } catch { }
        }
    }
    return $items
}

function Read-JsonLines {
    param([string]$Path)
    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) { return $items }
    foreach ($line in Get-Content -LiteralPath $Path) {
        if (-not [string]::IsNullOrWhiteSpace($line)) {
            try { $items += ($line | ConvertFrom-Json) } catch { }
        }
    }
    return $items
}

function Write-JsonLine {
    param([string]$Path, [object]$Value)
    Add-Content -LiteralPath $Path -Value ($Value | ConvertTo-Json -Compress -Depth 40)
}

function Get-ScenarioValue {
    param([string]$Path, [string]$Name)
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) { return $null }
    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) { continue }
        $parts = $trimmed.Split("=", 2)
        if ($parts[0].Trim() -eq $Name) { return $parts[1].Trim() }
    }
    return $null
}

function Resolve-WorkloadProfile {
    $rate = [double]$ProducerRatePerSecond
    $payload = $ProducerPayloadSizeBytes
    if (-not [string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $scenarioPath = (Resolve-Path -LiteralPath $ProducerScenarioFile).Path
        $baseRate = Get-ScenarioValue -Path $scenarioPath -Name "workload.rate-per-second"
        $peakRate = Get-ScenarioValue -Path $scenarioPath -Name "workload.peak-rate-per-second"
        $payloadValue = Get-ScenarioValue -Path $scenarioPath -Name "workload.payload-size-bytes"
        if ($null -ne $baseRate) { $rate = [double]$baseRate }
        if ($null -ne $peakRate) { $rate = [Math]::Max($rate, [double]$peakRate) }
        if ($null -ne $payloadValue) { $payload = [int]$payloadValue }
    }
    return [pscustomobject]@{ incomingRatePerSecond = $rate; payloadSizeBytes = $payload }
}

function Select-ConsumerConfiguration {
    param([int]$PayloadSizeBytes)
    if ($PayloadSizeBytes -le 512) {
        return [pscustomobject]@{ maxPollRecords = 500; fetchMinBytes = 1; maxPartitionFetchBytes = 1048576; reason = "small_payload_larger_batches" }
    }
    if ($PayloadSizeBytes -le 4096) {
        return [pscustomobject]@{ maxPollRecords = 200; fetchMinBytes = 1; maxPartitionFetchBytes = 4194304; reason = "medium_payload_balanced_batches" }
    }
    return [pscustomobject]@{ maxPollRecords = 50; fetchMinBytes = 1; maxPartitionFetchBytes = 8388608; reason = "large_payload_smaller_batches" }
}

function Get-ComposeNetwork {
    $network = (& docker network ls --format "{{.Name}}" | Where-Object { $_ -eq "dissertation_default" } | Select-Object -First 1)
    if ([string]::IsNullOrWhiteSpace($network)) {
        throw "Docker network dissertation_default was not found. Start Kafka with docker-compose first."
    }
    return $network
}

function Get-ActiveConsumerRuns {
    return @($script:consumerRuns | Where-Object { $null -eq $_.stoppedAt })
}

function Get-ActiveConsumerCount { return @(Get-ActiveConsumerRuns).Count }

function Start-ConsumerContainer {
    param([int]$Index, [string]$Reason)
    $containerName = "$RunName-consumer-$Index"
    $startedAt = New-UtcTimestamp
    $arguments = @(
        "run", "-d",
        "--name", $containerName,
        "--network", $composeNetwork,
        "--cpus", $ConsumerContainerCpus,
        "--memory", $ConsumerContainerMemory,
        "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:19092",
        "-e", "CONSUMER_TOPIC=$Topic",
        "-e", "CONSUMER_GROUP_ID=$ConsumerGroup",
        "-e", "CONSUMER_CPU_ITERATIONS=$ConsumerCpuIterations",
        "-e", "CONSUMER_PROCESSING_DELAY_MS=$ConsumerProcessingDelayMs",
        "-e", "CONSUMER_REPORT_INTERVAL_MS=$ConsumerReportIntervalMs",
        "-e", "CONSUMER_MAX_POLL_RECORDS=$ConsumerMaxPollRecords",
        "-e", "CONSUMER_FETCH_MIN_BYTES=$ConsumerFetchMinBytes",
        "-e", "CONSUMER_MAX_PARTITION_FETCH_BYTES=$ConsumerMaxPartitionFetchBytes",
        $ConsumerImage,
        "--spring.kafka.consumer.auto-offset-reset=latest",
        "--debug=false"
    )
    $containerId = (& docker @arguments).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($containerId)) {
        throw "Failed to start consumer container $containerName."
    }
    $run = [pscustomobject]@{
        index = $Index
        name = $containerName
        id = $containerId
        startedAt = $startedAt
        stoppedAt = $null
        stopReason = $null
        exitCode = $null
        logPath = Join-Path $logsDir "$containerName.log"
    }
    $script:consumerRuns += $run
    Write-JsonLine -Path $eventsPath -Value ([pscustomobject]@{
        timestamp = New-UtcTimestamp
        action = "start_container"
        consumerIndex = $Index
        containerName = $containerName
        activeConsumers = Get-ActiveConsumerCount
        reason = $Reason
    })
    return $run
}

function Save-ConsumerLog {
    param([object]$Run)
    $logText = & docker logs $Run.name 2>&1
    Set-Content -LiteralPath $Run.logPath -Value $logText -Encoding UTF8
}

function Stop-ConsumerContainer {
    param([object]$Run, [string]$Reason)
    if ($null -ne $Run.stoppedAt) { return }
    $stopStarted = Get-Date
    & docker stop --time 20 $Run.name | Out-Null
    $stopFinished = Get-Date
    Save-ConsumerLog -Run $Run
    $Run.exitCode = (& docker inspect $Run.name --format "{{.State.ExitCode}}" 2>$null)
    & docker rm $Run.name | Out-Null
    $Run.stoppedAt = New-UtcTimestamp
    $Run.stopReason = $Reason
    $rebalanceEstimateMs = ($stopFinished - $stopStarted).TotalMilliseconds
    Write-JsonLine -Path $eventsPath -Value ([pscustomobject]@{
        timestamp = New-UtcTimestamp
        action = "stop_container"
        consumerIndex = $Run.index
        containerName = $Run.name
        activeConsumers = Get-ActiveConsumerCount
        reason = $Reason
        stopDurationMillis = $rebalanceEstimateMs
    })
}

function Read-LagSample {
    $rows = @()
    $state = "ok"
    $message = $null
    try {
        $output = & docker-compose -f docker/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group $ConsumerGroup 2>&1
        if ($LASTEXITCODE -ne 0) { $state = "command_failed"; $message = ($output -join "`n") }
        foreach ($line in $output) {
            $trimmed = $line.Trim()
            if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("GROUP ")) { continue }
            $parts = $trimmed -split "\s+"
            if ($parts.Count -lt 6 -or $parts[1] -ne $Topic) { continue }
            $partition = 0; $current = 0L; $end = 0L; $lag = 0L
            if (-not [int]::TryParse($parts[2], [ref]$partition)) { continue }
            [void][long]::TryParse($parts[3], [ref]$current)
            if (-not [long]::TryParse($parts[4], [ref]$end)) { continue }
            if (-not [long]::TryParse($parts[5], [ref]$lag)) { continue }
            $rows += [pscustomobject]@{ partition = $partition; currentOffset = $current; logEndOffset = $end; lag = $lag }
        }
    } catch {
        $state = "exception"
        $message = $_.Exception.Message
    }
    $sample = [pscustomobject]@{
        schemaVersion = 1
        timestamp = New-UtcTimestamp
        groupId = $ConsumerGroup
        topic = $Topic
        state = $state
        message = $message
        activeConsumers = Get-ActiveConsumerCount
        partitions = @($rows).Count
        lag = if (@($rows).Count -gt 0) { ($rows | Measure-Object -Property lag -Sum).Sum } else { $null }
        rows = $rows
    }
    Write-JsonLine -Path $lagPath -Value $sample
    return $sample
}

function Wait-ForContainerGroupReady {
    param([int]$TimeoutSeconds)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $assigned = New-Object System.Collections.Generic.HashSet[string]
        $running = 0
        foreach ($run in Get-ActiveConsumerRuns) {
            $logText = & docker logs $run.name --tail 120 2>&1
            foreach ($line in $logText) {
                if (-not $line.StartsWith("CONSUMER_METRICS ")) { continue }
                try {
                    $metric = $line.Substring("CONSUMER_METRICS ".Length) | ConvertFrom-Json
                    if ($metric.listenerRunning) {
                        $running++
                        foreach ($partition in $metric.metrics.assignedPartitions) {
                            [void]$assigned.Add([string]$partition)
                        }
                    }
                } catch { }
            }
        }
        [void](Read-LagSample)
        if ($running -gt 0 -and $assigned.Count -ge [Math]::Min($TopicPartitions, (Get-ActiveConsumerCount))) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Start-Producer {
    if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $args = @("-jar", $producerJar, "--workload.topic=$Topic", "--workload.event-count=$ProducerEventCount",
            "--workload.rate-per-second=$ProducerRatePerSecond", "--workload.payload-size-bytes=$ProducerPayloadSizeBytes", "--debug=false")
    } else {
        $scenarioPath = (Resolve-Path -LiteralPath $ProducerScenarioFile).Path
        $args = @("-jar", $producerJar, "--workload.topic=$Topic", "--spring.config.additional-location=file:$scenarioPath", "--debug=false")
    }
    Write-JavaWrapper -Path $producerWrapper -ExitCodePath $producerExitCodePath -Arguments $args
    return Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $producerWrapper) `
        -RedirectStandardOutput $producerOut `
        -RedirectStandardError $producerErr `
        -PassThru `
        -WindowStyle Hidden
}

function Get-ConsumerMetricSummaries {
    $items = @()
    foreach ($run in $script:consumerRuns) {
        if (Test-Path -LiteralPath $run.logPath) {
            $reports = Read-PrefixedJson -Path $run.logPath -Prefix "CONSUMER_METRICS "
        } else {
            Save-ConsumerLog -Run $run
            $reports = Read-PrefixedJson -Path $run.logPath -Prefix "CONSUMER_METRICS "
        }
        $latest = @($reports) | Select-Object -Last 1
        $items += [pscustomobject]@{
            index = $run.index
            containerName = $run.name
            startedAt = $run.startedAt
            stoppedAt = $run.stoppedAt
            stopReason = $run.stopReason
            reports = @($reports).Count
            latestMetrics = $latest
            log = $run.logPath
            exitCode = $run.exitCode
        }
    }
    return $items
}

function New-ConsumerAggregate {
    param([object[]]$Metrics)
    $metricObjects = @($Metrics | Where-Object { $null -ne $_.latestMetrics } | ForEach-Object { $_.latestMetrics.metrics })
    return [pscustomobject]@{
        containersStarted = @($Metrics).Count
        processed = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processed -Sum).Sum } else { 0 }
        failed = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property failed -Sum).Sum } else { 0 }
        processedPayloadBytes = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processedPayloadBytes -Sum).Sum } else { 0 }
        processedValueBytes = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processedValueBytes -Sum).Sum } else { 0 }
    }
}

$workload = Resolve-WorkloadProfile
$selectedConsumerConfig = if ($ControllerMode -eq "Adaptive") {
    Select-ConsumerConfiguration -PayloadSizeBytes $workload.payloadSizeBytes
} else {
    [pscustomobject]@{ maxPollRecords = $ConsumerMaxPollRecords; fetchMinBytes = $ConsumerFetchMinBytes; maxPartitionFetchBytes = $ConsumerMaxPartitionFetchBytes; reason = "fixed_configuration" }
}
$ConsumerMaxPollRecords = $selectedConsumerConfig.maxPollRecords
$ConsumerFetchMinBytes = $selectedConsumerConfig.fetchMinBytes
$ConsumerMaxPartitionFetchBytes = $selectedConsumerConfig.maxPartitionFetchBytes
if ($ControllerMode -eq "Static") { $MinConsumers = $FixedConsumers; $MaxConsumers = $FixedConsumers }
if ($MaxConsumers -lt $MinConsumers) { throw "MaxConsumers must be greater than or equal to MinConsumers." }
if ($MaxConsumers -gt $TopicPartitions) { throw "MaxConsumers cannot exceed TopicPartitions for this experiment profile." }

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$runDir = Join-Path (Join-Path $repoRoot $ResultsRoot) $RunName
$logsDir = Join-Path $runDir "logs"
$samplesDir = Join-Path $runDir "samples"
if (Test-Path -LiteralPath $runDir) { throw "Run directory already exists: $runDir. Choose a new -RunName." }
New-Item -ItemType Directory -Force -Path $logsDir, $samplesDir | Out-Null

$producerJar = Join-Path $repoRoot "producer/target/telecom-producer-0.1.0-SNAPSHOT.jar"
$consumerJar = Join-Path $repoRoot "consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar"
$producerOut = Join-Path $logsDir "producer.out.log"
$producerErr = Join-Path $logsDir "producer.err.log"
$producerWrapper = Join-Path $logsDir "producer.run.ps1"
$producerExitCodePath = Join-Path $logsDir "producer.exitcode"
$lagPath = Join-Path $samplesDir "kafka-lag.jsonl"
$eventsPath = Join-Path $samplesDir "docker-scaling-events.jsonl"
$metadataPath = Join-Path $runDir "metadata.json"
$summaryPath = Join-Path $runDir "docker-scaling-summary.json"
$script:consumerRuns = @()
$producerProcess = $null
$nextConsumerIndex = 1

Push-Location -LiteralPath $repoRoot
try {
    if (-not $SkipBuild) {
        & mvn -B -ntp -f producer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Producer package failed." }
        & mvn -B -ntp -f consumer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Consumer package failed." }
    }
    if (-not $SkipKafkaStart) {
        & docker-compose -f docker/docker-compose.yml up -d --wait kafka
        if ($LASTEXITCODE -ne 0) { throw "Kafka startup failed." }
        & docker-compose -f docker/docker-compose.yml run --rm kafka-init
        if ($LASTEXITCODE -ne 0) { throw "Kafka topic initialization failed." }
    }
    if (-not $SkipImageBuild) {
        & docker build -t $ConsumerImage consumer
        if ($LASTEXITCODE -ne 0) { throw "Consumer image build failed." }
    }
    if (-not (Test-Path -LiteralPath $producerJar)) { throw "Producer jar not found at $producerJar." }

    $composeNetwork = Get-ComposeNetwork
    $metadata = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        type = "docker-consumer-approach"
        controllerMode = $ControllerMode.ToLowerInvariant()
        createdAt = New-UtcTimestamp
        topic = $Topic
        topicPartitions = $TopicPartitions
        consumerGroup = $ConsumerGroup
        image = $ConsumerImage
        containerLimits = [pscustomobject]@{ cpus = $ConsumerContainerCpus; memory = $ConsumerContainerMemory }
        workload = $workload
        selectedConsumerConfig = $selectedConsumerConfig
        consumerConfig = [pscustomobject]@{
            cpuIterations = $ConsumerCpuIterations
            processingDelayMs = $ConsumerProcessingDelayMs
            reportIntervalMs = $ConsumerReportIntervalMs
            maxPollRecords = $ConsumerMaxPollRecords
            fetchMinBytes = $ConsumerFetchMinBytes
            maxPartitionFetchBytes = $ConsumerMaxPartitionFetchBytes
        }
        controller = [pscustomobject]@{
            minConsumers = $MinConsumers
            maxConsumers = $MaxConsumers
            scaleUpLagThreshold = $ScaleUpLagThreshold
            scaleDownLagThreshold = $ScaleDownLagThreshold
            cooldownSeconds = $CooldownSeconds
            sampleIntervalSeconds = $SampleIntervalSeconds
            initialCapacityPerConsumer = $InitialCapacityPerConsumer
            capacityHeadroom = $CapacityHeadroom
            targetLatencyP95Millis = $TargetLatencyP95Millis
        }
    }
    $metadata | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

    for ($i = 1; $i -le $MinConsumers; $i++) {
        Start-ConsumerContainer -Index $nextConsumerIndex -Reason "minimum_capacity"
        $nextConsumerIndex++
    }
    if (-not (Wait-ForContainerGroupReady -TimeoutSeconds 90)) {
        throw "Initial Docker consumer group did not become ready within 90 seconds."
    }

    if ($ControllerMode -eq "Adaptive") {
        $initialDesired = [int][Math]::Ceiling(($workload.incomingRatePerSecond * $CapacityHeadroom) / [Math]::Max(1.0, $InitialCapacityPerConsumer))
        $initialDesired = [Math]::Max($MinConsumers, [Math]::Min($MaxConsumers, $initialDesired))
        while ((Get-ActiveConsumerCount) -lt $initialDesired) {
            Start-ConsumerContainer -Index $nextConsumerIndex -Reason "initial_workload_capacity_plan" | Out-Null
            $nextConsumerIndex++
        }
        if ($initialDesired -gt $MinConsumers -and -not (Wait-ForContainerGroupReady -TimeoutSeconds 90)) {
            throw "Adaptive Docker consumer group did not rebalance within 90 seconds."
        }
    }

    $producerProcess = Start-Producer
    $lastScaleAt = (Get-Date).AddSeconds(-1 * $CooldownSeconds)
    $producerExitedAt = $null
    $drainDeadline = $null

    while ($true) {
        $sample = Read-LagSample
        $lag = if ($null -eq $sample.lag) { 0L } else { [int64]$sample.lag }
        $active = Get-ActiveConsumerCount
        $producerExited = $producerProcess.HasExited
        if ($producerExited -and $null -eq $producerExitedAt) { $producerExitedAt = Get-Date }
        $cooldownElapsed = ((Get-Date) - $lastScaleAt).TotalSeconds -ge $CooldownSeconds

        if ($ControllerMode -ne "Static") {
            $shouldScaleUp = $lag -gt $ScaleUpLagThreshold
            $scaleReason = if ($ControllerMode -eq "Adaptive") { "adaptive_lag_pressure" } else { "lag_above_threshold" }
            if ($shouldScaleUp -and $active -lt $MaxConsumers -and $cooldownElapsed) {
                Start-ConsumerContainer -Index $nextConsumerIndex -Reason $scaleReason | Out-Null
                $nextConsumerIndex++
                $lastScaleAt = Get-Date
            } elseif ($producerExited -and $lag -le $ScaleDownLagThreshold -and $active -gt $MinConsumers -and $cooldownElapsed) {
                $candidate = @(Get-ActiveConsumerRuns | Sort-Object index -Descending | Select-Object -First 1)[0]
                Stop-ConsumerContainer -Run $candidate -Reason "drained_after_producer"
                $lastScaleAt = Get-Date
            }
        }

        if ($producerExited -and $lag -le $ScaleDownLagThreshold) {
            if ($null -eq $drainDeadline) {
                $drainDeadline = (Get-Date).AddSeconds($SampleIntervalSeconds * 2)
            } elseif ((Get-Date) -gt $drainDeadline) {
                break
            }
        } else {
            $drainDeadline = $null
        }
        if ($producerExited -and $null -ne $producerExitedAt -and ((Get-Date) - $producerExitedAt).TotalSeconds -gt $DrainTimeoutSeconds) { break }
        Start-Sleep -Seconds $SampleIntervalSeconds
    }

    $producerProcess.WaitForExit()
    $producerExitCode = Read-ExitCode -Path $producerExitCodePath
    foreach ($run in @(Get-ActiveConsumerRuns)) { Stop-ConsumerContainer -Run $run -Reason "run_complete" }

    $producerSummaries = Read-PrefixedJson -Path $producerOut -Prefix "PRODUCER_SUMMARY "
    $lagSamples = Read-JsonLines -Path $lagPath
    $events = Read-JsonLines -Path $eventsPath
    $consumerMetrics = Get-ConsumerMetricSummaries
    $aggregate = New-ConsumerAggregate -Metrics $consumerMetrics
    $validLagSamples = @($lagSamples | Where-Object { $_.state -eq "ok" -and $null -ne $_.lag })
    $peakLag = if ($validLagSamples.Count -gt 0) { ($validLagSamples | Measure-Object -Property lag -Maximum).Maximum } else { $null }
    $finalLag = @($validLagSamples | Select-Object -Last 1)[0]
    $maxObservedConsumers = if ($validLagSamples.Count -gt 0) { ($validLagSamples | Measure-Object -Property activeConsumers -Maximum).Maximum } else { $MinConsumers }

    $summary = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        controllerMode = $ControllerMode.ToLowerInvariant()
        completedAt = New-UtcTimestamp
        status = if ($producerExitCode -eq 0) { "completed" } else { "completed_with_nonzero_exit" }
        topic = $Topic
        consumerGroup = $ConsumerGroup
        producerExitCode = $producerExitCode
        producerSummary = @($producerSummaries) | Select-Object -Last 1
        workload = $workload
        selectedConsumerConfig = $selectedConsumerConfig
        scalingEvents = $events
        consumerContainerMetrics = $consumerMetrics
        aggregateConsumerMetrics = $aggregate
        peakKafkaLag = $peakLag
        finalKafkaLag = $finalLag
        maxObservedConsumers = $maxObservedConsumers
        files = [pscustomobject]@{
            metadata = $metadataPath
            producerOut = $producerOut
            kafkaLag = $lagPath
            scalingEvents = $eventsPath
            summary = $summaryPath
        }
    }
    $summary | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
    Write-Output "DOCKER_SCALING_SUMMARY $summaryPath"
    Write-Output ($summary | ConvertTo-Json -Compress -Depth 60)
} finally {
    if ($null -ne $producerProcess -and -not $producerProcess.HasExited) { Stop-Process -Id $producerProcess.Id -Force -ErrorAction SilentlyContinue }
    if ($null -ne (Get-Variable -Name consumerRuns -Scope Script -ErrorAction SilentlyContinue).Value) {
        foreach ($run in $script:consumerRuns) {
            if ($null -eq $run.stoppedAt) {
                try { Stop-ConsumerContainer -Run $run -Reason "cleanup" } catch { }
            }
        }
    }
    Pop-Location
}
