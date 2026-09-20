param(
    [ValidateRange(1, 100000)]
    [int[]]$Rates = @(20, 50),
    [ValidateRange(1, 10)]
    [int]$Repetitions = 2,
    [ValidateRange(2, 1000000)]
    [int]$EventCount = 201,
    [ValidateRange(0, 262144)]
    [int]$PayloadSizeBytes = 256,
    [string]$BootstrapServers = 'localhost:9092',
    [string]$Topic = 'telecom-events'
)

$ErrorActionPreference = 'Stop'
$jar = Join-Path $PSScriptRoot 'target/telecom-producer-0.1.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar)) {
    throw 'Build the producer first: mvn -B -ntp -f producer/pom.xml package'
}
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$outputFile = Join-Path $PSScriptRoot "target/pacing-calibration-$stamp.json"
$runs = [System.Collections.Generic.List[object]]::new()

foreach ($rate in $Rates) {
    for ($repeat = 1; $repeat -le $Repetitions; $repeat++) {
        Write-Host "Calibrating target=$rate events/s, repetition=$repeat, count=$EventCount"
        $runOutput = @(& java -jar $jar "--workload.rate-per-second=$rate" `
            "--workload.event-count=$EventCount" "--workload.payload-size-bytes=$PayloadSizeBytes" `
            "--spring.kafka.bootstrap-servers=$BootstrapServers" "--workload.topic=$Topic" --debug=false)
        if ($LASTEXITCODE -ne 0) {
            $runOutput | Write-Host
            throw "Producer exited with code $LASTEXITCODE. Earlier results, if any, are in $outputFile"
        }
        $summaryLines = @($runOutput | Where-Object { $_ -like 'PRODUCER_SUMMARY *' })
        if ($summaryLines.Count -ne 1) {
            throw 'Expected exactly one producer summary.'
        }
        $summary = $summaryLines[0].Substring('PRODUCER_SUMMARY '.Length) | ConvertFrom-Json
        if ($summary.acknowledged -ne $EventCount -or $null -eq $summary.eventStartRatePerSecond) {
            throw 'Run did not acknowledge every event or is missing pacing diagnostics. Rebuild the producer.'
        }
        $errorPercent = 100 * ($summary.eventStartRatePerSecond - $rate) / $rate
        $runs.Add([pscustomobject]@{
            repetition = $repeat
            eventStartRateErrorPercent = $errorPercent
            withinTenPercent = [Math]::Abs($errorPercent) -le 10
            summary = $summary
        })
        [pscustomobject]@{
            recordedAt = [DateTimeOffset]::UtcNow.ToString('o')
            bootstrapServers = $BootstrapServers
            plannedRuns = $Rates.Count * $Repetitions
            completedRuns = $runs.Count
            runs = @($runs.ToArray())
        } | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $outputFile -Encoding UTF8
        Write-Host ('Observed={0:F2} events/s, error={1:F2}%, resets={2}, acknowledged={3}' -f `
            $summary.eventStartRatePerSecond, $errorPercent, $summary.pacingResetCount, $summary.acknowledged)
    }
}
Write-Host "Calibration results: $outputFile"
Write-Host 'The JSON flags rates outside 10% of target. This is calibration, not a benchmark acceptance test.'
