$ErrorActionPreference = 'Stop'
$composeFile = Join-Path $PSScriptRoot 'docker-compose.yml'
$topic = 'smoke-' + [guid]::NewGuid().ToString('N')
$message = '{"eventId":"smoke-test","eventType":"CALL_RECORD","customerRegion":"north"}'
$created = $false

function Invoke-Kafka {
    param([string[]]$KafkaArguments)
    & docker-compose -f $composeFile exec -T -e 'KAFKA_HEAP_OPTS=-Xms64m -Xmx256m' kafka @KafkaArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka command failed with exit code $LASTEXITCODE."
    }
}

try {
    Invoke-Kafka -KafkaArguments @(
        '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:19092',
        '--create', '--topic', $topic, '--partitions', '1', '--replication-factor', '1'
    )
    $created = $true

    $message | & docker-compose -f $composeFile exec -T -e 'KAFKA_HEAP_OPTS=-Xms64m -Xmx256m' kafka `
        /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 `
        --topic $topic --producer-property acks=all --producer-property delivery.timeout.ms=15000 `
        --producer-property request.timeout.ms=10000 --producer-property max.block.ms=15000
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to produce the test message.'
    }

    $received = @(Invoke-Kafka -KafkaArguments @(
        '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server', 'kafka:19092',
        '--topic', $topic, '--partition', '0', '--offset', 'earliest',
        '--max-messages', '1', '--timeout-ms', '15000'
    ))
    if ($received -notcontains $message) {
        throw "Consumed message did not match the produced message: $received"
    }

    Invoke-Kafka -KafkaArguments @(
        '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:19092',
        '--describe', '--topic', 'telecom-events'
    )
    Write-Host "PASS: produced and consumed an identical message on $topic."
}
finally {
    if ($created) {
        Invoke-Kafka -KafkaArguments @(
            '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:19092',
            '--delete', '--topic', $topic
        )
    }
}
