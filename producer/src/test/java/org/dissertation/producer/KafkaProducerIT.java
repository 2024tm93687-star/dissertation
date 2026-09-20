package org.dissertation.producer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class KafkaProducerIT {
    @ParameterizedTest
    @ValueSource(strings = {"count", "steady", "ramp", "burst"})
    void verifiesDeliveredWorkloadAndPhaseReports(String mode, CapturedOutput output) throws Exception {
        String bootstrap = System.getProperty("kafka.bootstrap", "localhost:9092");
        String topic = "producer-it-" + UUID.randomUUID();
        String runId = UUID.randomUUID().toString();
        var mapper = JsonMapper.builder().build();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap,
                "default.api.timeout.ms", "15000", "request.timeout.ms", "10000"))) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(20, TimeUnit.SECONDS);
            try {
                long before = System.currentTimeMillis();
                var arguments = new ArrayList<>(List.of("--spring.kafka.bootstrap-servers=" + bootstrap,
                        "--workload.topic=" + topic, "--workload.run-id=" + runId,
                        "--workload.rate-per-second=20", "--workload.payload-size-bytes=256",
                        "--workload.seed=42", "--debug=false"));
                if (mode.equals("count")) {
                    arguments.add("--workload.event-count=30");
                } else {
                    arguments.add("--workload.duration=4s");
                    arguments.add("--workload.pattern=" + mode);
                    if (!mode.equals("steady")) {
                        arguments.add("--workload.peak-rate-per-second=40");
                    }
                    if (mode.equals("burst")) {
                        arguments.addAll(List.of("--workload.burst-start=1s", "--workload.burst-duration=1s",
                                "--workload.quiet-duration=1s"));
                    }
                }
                WorkloadProperties applied;
                try (var context = SpringApplication.run(ProducerApplication.class, arguments.toArray(String[]::new))) {
                    assertThat(context.isActive()).isTrue();
                    applied = context.getBean(WorkloadProperties.class);
                }
                String summaryJson = output.getOut().lines().filter(line -> line.startsWith("PRODUCER_SUMMARY "))
                        .reduce((first, second) -> second).orElseThrow().substring("PRODUCER_SUMMARY ".length());
                var summary = mapper.readValue(summaryJson, RunSummary.class);
                var timeline = applied.duration() == null ? null : new WorkloadTimeline(applied);
                assertThat(summary.sent()).isPositive().isEqualTo(summary.acknowledged());
                if (timeline == null) {
                    assertThat(summary.sent()).isEqualTo(30);
                    assertThat(summary.requested()).isEqualTo(30);
                } else {
                    assertThat(summary.requested()).isNull();
                    assertThat(summary.requestedDurationSeconds()).isEqualTo(4);
                    assertThat(summary.elapsedSeconds()).isGreaterThanOrEqualTo(4);
                }
                long after = System.currentTimeMillis();
                Map<String, Object> settings = Map.of("bootstrap.servers", bootstrap,
                        "key.deserializer", StringDeserializer.class,
                        "value.deserializer", StringDeserializer.class,
                        "enable.auto.commit", false, "allow.auto.create.topics", false,
                        "default.api.timeout.ms", 15000);
                try (var consumer = new KafkaConsumer<String, String>(settings)) {
                    var partitions = List.of(new TopicPartition(topic, 0),
                            new TopicPartition(topic, 1), new TopicPartition(topic, 2));
                    consumer.assign(partitions);
                    consumer.seekToBeginning(partitions);
                    var endOffsets = consumer.endOffsets(partitions);
                    var received = new ArrayList<TelecomEvent>();
                    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                    while (partitions.stream().anyMatch(p -> consumer.position(p) < endOffsets.get(p))) {
                        assertThat(System.nanoTime()).as("consumer deadline").isLessThan(deadline);
                        for (var record : consumer.poll(Duration.ofMillis(250))) {
                            var event = mapper.readValue(record.value(), TelecomEvent.class);
                            assertThat(record.key()).isEqualTo(event.subscriberId());
                            assertThat(event.runId()).isEqualTo(runId);
                            assertThat(event.createdAt()).isBetween(before, after);
                            assertThat(event.payloadSizeBytes()).isEqualTo(256);
                            assertThat(event.payload().getBytes(StandardCharsets.UTF_8)).hasSize(256);
                            assertThat(event.workloadElapsedNanos()).isNotNegative();
                            if (timeline != null) {
                                assertThat(event.workloadElapsedNanos()).isLessThan(timeline.durationNanos());
                                var phase = timeline.phases().get(timeline.phaseIndexAt(event.workloadElapsedNanos()));
                                assertThat(event.workloadPhase()).isEqualTo(phase.name()).isNotEqualTo("quiet");
                            }
                            received.add(event);
                        }
                    }
                    assertThat(received).hasSize((int) summary.sent());
                    assertThat(received).extracting(TelecomEvent::eventId).doesNotHaveDuplicates();
                    received.sort(java.util.Comparator.comparingLong(TelecomEvent::sequence));
                    var expected = new EventFactory(runId, 256, 42);
                    for (int i = 0; i < received.size(); i++) {
                        var event = received.get(i);
                        assertThat(event).isEqualTo(expected.create(i, event.createdAt(),
                                event.workloadPhase(), event.workloadElapsedNanos()));
                    }
                    // Broad lower bound checks pacing without depending on host scheduling precision.
                    assertThat(received.getLast().createdAt() - received.getFirst().createdAt())
                            .isGreaterThanOrEqualTo(1300);
                    for (var observation : summary.phases()) {
                        long count = received.stream().filter(event -> event.workloadPhase().equals(observation.name())).count();
                        verifyObservation(observation, count);
                        if (observation.name().equals("quiet")) {
                            assertThat(count).isZero();
                        } else {
                            assertThat(count).isPositive();
                        }
                    }
                    for (var window : summary.rateWindows()) {
                        long count = received.stream().filter(event -> {
                            double seconds = event.workloadElapsedNanos() / 1e9;
                            return seconds >= window.startSeconds() && seconds < window.endSeconds();
                        }).count();
                        verifyObservation(window, count);
                    }
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
            }
        }
    }

    private void verifyObservation(RateObservation observation, long count) {
        assertThat(observation.sent()).isEqualTo(count);
        assertThat(observation.acknowledged()).isEqualTo(count);
        assertThat(observation.observedRatePerSecond())
                .isEqualTo(count / (observation.endSeconds() - observation.startSeconds()));
    }
}
