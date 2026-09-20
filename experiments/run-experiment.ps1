[CmdletBinding()]
param(
    [string]$RunName = ("run-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")),
    [string]$ResultsRoot = "results",
    [string]$Topic = "telecom-events",
    [ValidateRange(1, 1000)]
    [int]$TopicPartitions = 6,
    [string]$ConsumerGroup = ("experiment-" + [guid]::NewGuid().ToString("N")),
    [ValidateRange(1, 32)]
    [int]$ConsumerInstances = 1,
    [int]$ConsumerRunDurationSeconds = 45,
    [int]$ConsumerCpuIterations = 1000,
    [int]$ConsumerProcessingDelayMs = 0,
    [int]$ConsumerReportIntervalMs = 2000,
    [switch]$EnableConsumerMetricsHttp,
    [int]$ConsumerHttpPort = 18081,
    [int]$SampleIntervalSeconds = 2,
    [string]$ProducerScenarioFile = "",
    [int]$ProducerEventCount = 120,
    [int]$ProducerRatePerSecond = 40,
    [int]$ProducerPayloadSizeBytes = 256,
    [switch]$SkipKafkaStart,
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Convert-ToJsonLine {
    param([object]$Value)
    return ($Value | ConvertTo-Json -Compress -Depth 30)
}

function New-UtcTimestamp {
    return (Get-Date).ToUniversalTime().ToString("o")
}

function Convert-ToPowerShellLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Write-JavaWrapper {
    param(
        [string]$Path,
        [string]$ExitCodePath,
        [string[]]$Arguments
    )

    $lines = @()
    $lines += '$ErrorActionPreference = "Continue"'
    $lines += '$javaArgs = @('
    foreach ($argument in $Arguments) {
        $lines += "    $(Convert-ToPowerShellLiteral $argument)"
    }
    $lines += ')'
    $lines += '& java @javaArgs'
    $lines += '$exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }'
    $lines += "Set-Content -LiteralPath $(Convert-ToPowerShellLiteral $ExitCodePath) -Value `$exitCode -Encoding ASCII"
    $lines += 'exit $exitCode'
    Set-Content -LiteralPath $Path -Value $lines -Encoding UTF8
}

function Read-ExitCode {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $value = (Get-Content -LiteralPath $Path -TotalCount 1).Trim()
    $code = 0
    if ([int]::TryParse($value, [ref]$code)) {
        return $code
    }
    return $null
}

function Read-PrefixedJson {
    param(
        [string]$Path,
        [string]$Prefix
    )

    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) {
        return $items
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line.StartsWith($Prefix)) {
            $json = $line.Substring($Prefix.Length).Trim()
            try {
                $items += ($json | ConvertFrom-Json)
            } catch {
                # Keep parsing the rest of the run output; malformed log lines are visible in raw files.
            }
        }
    }
    return $items
}

function Read-JsonLines {
    param([string]$Path)

    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) {
        return $items
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        if (-not [string]::IsNullOrWhiteSpace($line)) {
            try {
                $items += ($line | ConvertFrom-Json)
            } catch {
                # Keep parsing the rest of the file; raw output is preserved.
            }
        }
    }
    return $items
}

function Wait-ForConsumerAssignment {
    param(
        [string]$LogPath,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path -LiteralPath $LogPath) {
            $reports = Read-PrefixedJson -Path $LogPath -Prefix "CONSUMER_METRICS "
            $latest = @($reports) | Select-Object -Last 1
            if ($null -ne $latest -and $latest.listenerRunning -and $latest.metrics.assignedPartitions.Count -gt 0) {
                return $true
            }
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Wait-ForConsumerGroupReady {
    param(
        [object[]]$ConsumerSpecs,
        [int]$ExpectedPartitions,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $runningConsumers = 0
        $assigned = New-Object System.Collections.Generic.HashSet[string]

        foreach ($spec in $ConsumerSpecs) {
            if (-not (Test-Path -LiteralPath $spec.out)) {
                continue
            }
            $reports = Read-PrefixedJson -Path $spec.out -Prefix "CONSUMER_METRICS "
            $latest = @($reports) | Select-Object -Last 1
            if ($null -eq $latest -or -not $latest.listenerRunning) {
                continue
            }

            $runningConsumers++
            foreach ($partition in $latest.metrics.assignedPartitions) {
                [void]$assigned.Add([string]$partition)
            }
        }

        if ($runningConsumers -eq @($ConsumerSpecs).Count -and $assigned.Count -ge $ExpectedPartitions) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function New-ConsumerAggregate {
    param([object[]]$InstanceMetrics)

    $finals = @($InstanceMetrics | Where-Object { $null -ne $_.finalMetrics })
    $metricObjects = @($finals | ForEach-Object { $_.finalMetrics.metrics })
    return [pscustomobject]@{
        instances = @($InstanceMetrics).Count
        instancesWithFinalMetrics = $finals.Count
        processed = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processed -Sum).Sum } else { 0 }
        failed = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property failed -Sum).Sum } else { 0 }
        processedPayloadBytes = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processedPayloadBytes -Sum).Sum } else { 0 }
        processedValueBytes = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processedValueBytes -Sum).Sum } else { 0 }
        futureTimestampEvents = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property futureTimestampEvents -Sum).Sum } else { 0 }
        assignmentEvents = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property assignmentEvents -Sum).Sum } else { 0 }
        revocationEvents = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property revocationEvents -Sum).Sum } else { 0 }
        lostEvents = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property lostEvents -Sum).Sum } else { 0 }
    }
}

function Start-LagSampler {
    param(
        [string]$RepoRoot,
        [string]$OutputPath,
        [string]$TopicName,
        [string]$GroupId,
        [int]$IntervalSeconds
    )

    return Start-Job -ArgumentList $RepoRoot, $OutputPath, $TopicName, $GroupId, $IntervalSeconds -ScriptBlock {
        param($RepoRoot, $OutputPath, $TopicName, $GroupId, $IntervalSeconds)
        Set-Location -LiteralPath $RepoRoot

        while ($true) {
            $timestamp = (Get-Date).ToUniversalTime().ToString("o")
            $rows = @()
            $state = "ok"
            $message = $null

            try {
                $output = & docker-compose -f docker/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group $GroupId 2>&1
                $exitCode = $LASTEXITCODE
                if ($exitCode -ne 0) {
                    $state = "command_failed"
                    $message = ($output -join "`n")
                }

                foreach ($line in $output) {
                    $trimmed = $line.Trim()
                    if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("GROUP ")) {
                        continue
                    }

                    $parts = $trimmed -split "\s+"
                    if ($parts.Count -lt 6 -or $parts[1] -ne $TopicName) {
                        continue
                    }

                    $partition = 0
                    if (-not [int]::TryParse($parts[2], [ref]$partition)) {
                        continue
                    }

                    $currentOffset = $null
                    $parsedCurrent = 0L
                    if ([long]::TryParse($parts[3], [ref]$parsedCurrent)) {
                        $currentOffset = $parsedCurrent
                    }

                    $logEndOffset = 0L
                    $lag = 0L
                    if (-not [long]::TryParse($parts[4], [ref]$logEndOffset)) {
                        continue
                    }
                    if (-not [long]::TryParse($parts[5], [ref]$lag)) {
                        continue
                    }

                    $rows += [pscustomobject]@{
                        partition = $partition
                        currentOffset = $currentOffset
                        logEndOffset = $logEndOffset
                        lag = $lag
                        consumerId = if ($parts.Count -gt 6) { $parts[6] } else { $null }
                        host = if ($parts.Count -gt 7) { $parts[7] } else { $null }
                        clientId = if ($parts.Count -gt 8) { $parts[8] } else { $null }
                    }
                }
            } catch {
                $state = "exception"
                $message = $_.Exception.Message
            }

            $numericCurrentRows = @($rows | Where-Object { $null -ne $_.currentOffset })
            $sample = [pscustomobject]@{
                schemaVersion = 1
                timestamp = $timestamp
                groupId = $GroupId
                topic = $TopicName
                state = $state
                message = $message
                partitions = @($rows).Count
                currentOffsetSum = if ($numericCurrentRows.Count -gt 0) { ($numericCurrentRows | Measure-Object -Property currentOffset -Sum).Sum } else { $null }
                logEndOffsetSum = if (@($rows).Count -gt 0) { ($rows | Measure-Object -Property logEndOffset -Sum).Sum } else { $null }
                lag = if (@($rows).Count -gt 0) { ($rows | Measure-Object -Property lag -Sum).Sum } else { $null }
                rows = $rows
            }

            Add-Content -LiteralPath $OutputPath -Value ($sample | ConvertTo-Json -Compress -Depth 20)
            Start-Sleep -Seconds $IntervalSeconds
        }
    }
}

function Start-DockerStatsSampler {
    param(
        [string]$OutputPath,
        [int]$IntervalSeconds
    )

    return Start-Job -ArgumentList $OutputPath, $IntervalSeconds -ScriptBlock {
        param($OutputPath, $IntervalSeconds)

        while ($true) {
            $timestamp = (Get-Date).ToUniversalTime().ToString("o")
            $containers = @()
            $state = "ok"
            $message = $null

            try {
                $output = & docker stats --no-stream --format "{{json .}}" 2>&1
                if ($LASTEXITCODE -ne 0) {
                    $state = "command_failed"
                    $message = ($output -join "`n")
                } else {
                    foreach ($line in $output) {
                        if (-not [string]::IsNullOrWhiteSpace($line)) {
                            $containers += ($line | ConvertFrom-Json)
                        }
                    }
                }
            } catch {
                $state = "exception"
                $message = $_.Exception.Message
            }

            $sample = [pscustomobject]@{
                schemaVersion = 1
                timestamp = $timestamp
                state = $state
                message = $message
                containers = $containers
            }

            Add-Content -LiteralPath $OutputPath -Value ($sample | ConvertTo-Json -Compress -Depth 20)
            Start-Sleep -Seconds $IntervalSeconds
        }
    }
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$resultsRootPath = Join-Path $repoRoot $ResultsRoot
$runDir = Join-Path $resultsRootPath $RunName
$logsDir = Join-Path $runDir "logs"
$samplesDir = Join-Path $runDir "samples"
if (Test-Path -LiteralPath $runDir) {
    throw "Run directory already exists: $runDir. Choose a new -RunName to keep result files isolated."
}
New-Item -ItemType Directory -Force -Path $logsDir, $samplesDir | Out-Null

$consumerSpecs = @()
for ($index = 1; $index -le $ConsumerInstances; $index++) {
    $name = if ($ConsumerInstances -eq 1) { "consumer" } else { "consumer-$index" }
    $consumerSpecs += [pscustomobject]@{
        index = $index
        name = $name
        out = Join-Path $logsDir "$name.out.log"
        err = Join-Path $logsDir "$name.err.log"
        wrapper = Join-Path $logsDir "$name.run.ps1"
        exitCodePath = Join-Path $logsDir "$name.exitcode"
        httpPort = $ConsumerHttpPort + $index - 1
    }
}
$producerOut = Join-Path $logsDir "producer.out.log"
$producerErr = Join-Path $logsDir "producer.err.log"
$producerWrapper = Join-Path $logsDir "producer.run.ps1"
$producerExitCodePath = Join-Path $logsDir "producer.exitcode"
$lagSamples = Join-Path $samplesDir "kafka-lag.jsonl"
$dockerSamples = Join-Path $samplesDir "docker-stats.jsonl"
$metadataPath = Join-Path $runDir "metadata.json"
$summaryPath = Join-Path $runDir "summary.json"

$consumerJar = Join-Path $repoRoot "consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar"
$producerJar = Join-Path $repoRoot "producer/target/telecom-producer-0.1.0-SNAPSHOT.jar"

Push-Location -LiteralPath $repoRoot
try {
    if (-not $SkipBuild) {
        & mvn -B -ntp -f producer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Producer package failed." }
        & mvn -B -ntp -f consumer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Consumer package failed." }
    }

    if (-not (Test-Path -LiteralPath $producerJar)) {
        throw "Producer jar not found at $producerJar. Run without -SkipBuild or build the producer first."
    }
    if (-not (Test-Path -LiteralPath $consumerJar)) {
        throw "Consumer jar not found at $consumerJar. Run without -SkipBuild or build the consumer first."
    }

    if (-not $SkipKafkaStart) {
        & docker-compose -f docker/docker-compose.yml up -d --wait kafka
        if ($LASTEXITCODE -ne 0) { throw "Kafka startup failed." }
        & docker-compose -f docker/docker-compose.yml run --rm kafka-init
        if ($LASTEXITCODE -ne 0) { throw "Kafka topic initialization failed." }
    }

    $metadata = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        createdAt = New-UtcTimestamp
        topic = $Topic
        topicPartitions = $TopicPartitions
        consumerGroup = $ConsumerGroup
        sampleIntervalSeconds = $SampleIntervalSeconds
        consumer = [pscustomobject]@{
            instances = $ConsumerInstances
            runDurationSeconds = $ConsumerRunDurationSeconds
            cpuIterations = $ConsumerCpuIterations
            processingDelayMs = $ConsumerProcessingDelayMs
            reportIntervalMs = $ConsumerReportIntervalMs
            metricsHttpEnabled = [bool]$EnableConsumerMetricsHttp
            httpPorts = if ($EnableConsumerMetricsHttp) { @($consumerSpecs | ForEach-Object { $_.httpPort }) } else { @() }
        }
        producer = [pscustomobject]@{
            scenarioFile = if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) { $null } else { $ProducerScenarioFile }
            eventCount = $ProducerEventCount
            ratePerSecond = $ProducerRatePerSecond
            payloadSizeBytes = $ProducerPayloadSizeBytes
        }
        files = [pscustomobject]@{
            consumerOut = @($consumerSpecs)[0].out
            consumerLogs = @($consumerSpecs | ForEach-Object {
                [pscustomobject]@{
                    index = $_.index
                    name = $_.name
                    out = $_.out
                    err = $_.err
                    exitCode = $_.exitCodePath
                }
            })
            producerOut = $producerOut
            kafkaLag = $lagSamples
            dockerStats = $dockerSamples
            summary = $summaryPath
        }
    }
    $metadata | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

    $lagJob = Start-LagSampler -RepoRoot $repoRoot -OutputPath $lagSamples -TopicName $Topic -GroupId $ConsumerGroup -IntervalSeconds $SampleIntervalSeconds
    $dockerJob = Start-DockerStatsSampler -OutputPath $dockerSamples -IntervalSeconds $SampleIntervalSeconds

    $consumerRuns = @()
    foreach ($spec in $consumerSpecs) {
        $consumerArgs = @(
            "-jar", $consumerJar,
            "--consumer.topic=$Topic",
            "--consumer.run-duration=${ConsumerRunDurationSeconds}s",
            "--consumer.cpu-iterations=$ConsumerCpuIterations",
            "--consumer.processing-delay-ms=$ConsumerProcessingDelayMs",
            "--consumer.report-interval-ms=$ConsumerReportIntervalMs",
            "--spring.kafka.consumer.group-id=$ConsumerGroup",
            "--spring.kafka.consumer.auto-offset-reset=latest",
            "--debug=false"
        )
        if ($EnableConsumerMetricsHttp) {
            $consumerArgs += "--spring.main.web-application-type=servlet"
            $consumerArgs += "--server.port=$($spec.httpPort)"
            $consumerArgs += "--management.server.port=$($spec.httpPort)"
        }
        Write-JavaWrapper -Path $spec.wrapper -ExitCodePath $spec.exitCodePath -Arguments $consumerArgs
        $process = Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $spec.wrapper) -RedirectStandardOutput $spec.out -RedirectStandardError $spec.err -PassThru -WindowStyle Hidden
        $consumerRuns += [pscustomobject]@{
            spec = $spec
            process = $process
        }
    }

    if (-not (Wait-ForConsumerGroupReady -ConsumerSpecs $consumerSpecs -ExpectedPartitions $TopicPartitions -TimeoutSeconds 90)) {
        $logHints = ($consumerSpecs | ForEach-Object { "$($_.name): $($_.out)" }) -join "; "
        throw "Consumer group did not assign $TopicPartitions partitions within 90 seconds. Logs: $logHints"
    }

    if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $producerArgs = @(
            "-jar", $producerJar,
            "--workload.topic=$Topic",
            "--workload.event-count=$ProducerEventCount",
            "--workload.rate-per-second=$ProducerRatePerSecond",
            "--workload.payload-size-bytes=$ProducerPayloadSizeBytes",
            "--debug=false"
        )
    } else {
        $scenarioPath = (Resolve-Path -LiteralPath $ProducerScenarioFile).Path
        $producerArgs = @(
            "-jar", $producerJar,
            "--workload.topic=$Topic",
            "--spring.config.additional-location=file:$scenarioPath",
            "--debug=false"
        )
    }

    Write-JavaWrapper -Path $producerWrapper -ExitCodePath $producerExitCodePath -Arguments $producerArgs
    $producerProcess = Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $producerWrapper) -RedirectStandardOutput $producerOut -RedirectStandardError $producerErr -PassThru -WindowStyle Hidden
    $producerProcess.WaitForExit()
    $producerProcess.Refresh()
    $producerExitCode = Read-ExitCode -Path $producerExitCodePath

    $consumerExitCodes = @()
    $consumerTimeoutMs = ($ConsumerRunDurationSeconds + 90) * 1000
    foreach ($run in $consumerRuns) {
        if (-not $run.process.WaitForExit($consumerTimeoutMs)) {
            Stop-Process -Id $run.process.Id -Force
            throw "Consumer $($run.spec.index) did not exit within expected timeout and was stopped."
        }
        $run.process.Refresh()
        $consumerExitCodes += [pscustomobject]@{
            index = $run.spec.index
            exitCode = Read-ExitCode -Path $run.spec.exitCodePath
        }
    }

    Start-Sleep -Seconds ($SampleIntervalSeconds + 1)

    $producerSummaries = Read-PrefixedJson -Path $producerOut -Prefix "PRODUCER_SUMMARY "
    $consumerInstanceMetrics = @()
    foreach ($spec in $consumerSpecs) {
        $reports = Read-PrefixedJson -Path $spec.out -Prefix "CONSUMER_METRICS "
        $final = @($reports | Where-Object { $_.finalSnapshot }) | Select-Object -Last 1
        if ($null -eq $final) {
            $final = @($reports) | Select-Object -Last 1
        }
        $consumerInstanceMetrics += [pscustomobject]@{
            index = $spec.index
            name = $spec.name
            reports = @($reports).Count
            exitCode = @($consumerExitCodes | Where-Object { $_.index -eq $spec.index } | Select-Object -First 1).exitCode
            finalMetrics = $final
        }
    }
    $consumerReports = @($consumerInstanceMetrics | ForEach-Object { $_.reports })
    $lagReports = Read-JsonLines -Path $lagSamples
    $dockerReports = Read-JsonLines -Path $dockerSamples

    $finalConsumer = @($consumerInstanceMetrics)[0].finalMetrics
    $aggregateConsumer = New-ConsumerAggregate -InstanceMetrics $consumerInstanceMetrics

    $finalLag = @($lagReports | Where-Object { $_.state -eq "ok" -and $null -ne $_.lag }) | Select-Object -Last 1
    $lastDockerStats = @($dockerReports | Where-Object { $_.state -eq "ok" }) | Select-Object -Last 1
    $allConsumerExitCodesZero = @($consumerExitCodes | Where-Object { $_.exitCode -ne 0 }).Count -eq 0

    $summary = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        completedAt = New-UtcTimestamp
        status = if ($producerExitCode -eq 0 -and $allConsumerExitCodesZero) { "completed" } else { "completed_with_nonzero_exit" }
        topic = $Topic
        consumerGroup = $ConsumerGroup
        producerExitCode = $producerExitCode
        consumerExitCode = if ($ConsumerInstances -eq 1) { @($consumerExitCodes)[0].exitCode } else { $null }
        consumerExitCodes = $consumerExitCodes
        producerSummary = @($producerSummaries) | Select-Object -Last 1
        finalConsumerMetrics = $finalConsumer
        consumerInstanceMetrics = $consumerInstanceMetrics
        aggregateConsumerMetrics = $aggregateConsumer
        finalKafkaLag = $finalLag
        lastDockerStats = $lastDockerStats
        sampleCounts = [pscustomobject]@{
            consumerReports = if ($consumerReports.Count -gt 0) { ($consumerReports | Measure-Object -Sum).Sum } else { 0 }
            lagReports = @($lagReports).Count
            dockerStatsReports = @($dockerReports).Count
        }
        files = $metadata.files
    }
    $summary | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $summaryPath -Encoding UTF8

    Write-Output "EXPERIMENT_SUMMARY $summaryPath"
    Write-Output (Convert-ToJsonLine $summary)
} finally {
    if ($null -ne (Get-Variable -Name consumerRuns -ErrorAction SilentlyContinue).Value) {
        foreach ($run in $consumerRuns) {
            if ($null -ne $run.process -and -not $run.process.HasExited) {
                Stop-Process -Id $run.process.Id -Force -ErrorAction SilentlyContinue
            }
        }
    }
    if ($null -ne (Get-Variable -Name lagJob -ErrorAction SilentlyContinue).Value) {
        Stop-Job -Job $lagJob -ErrorAction SilentlyContinue | Out-Null
        Remove-Job -Job $lagJob -Force -ErrorAction SilentlyContinue | Out-Null
    }
    if ($null -ne (Get-Variable -Name dockerJob -ErrorAction SilentlyContinue).Value) {
        Stop-Job -Job $dockerJob -ErrorAction SilentlyContinue | Out-Null
        Remove-Job -Job $dockerJob -Force -ErrorAction SilentlyContinue | Out-Null
    }
    Pop-Location
}
