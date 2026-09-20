package org.dissertation.consumer;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

class KafkaConsumerIT {
    private final String bootstrap = System.getProperty("kafka.bootstrap", "localhost:9092");

    @Test
    void processesPayloadsCommitsAndResumesWithoutReplayingCommittedRecords() throws Exception {
        String topic = "consumer-it-" + UUID.randomUUID();
        String group = "consumer-it-" + UUID.randomUUID();
        try (var admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(20, TimeUnit.SECONDS);
            try {
                publish(topic, 30, 3);
                try (var context = start(topic, group)) {
                    var metrics = context.getBean(ConsumerMetrics.class);
                    await(() -> metrics.snapshot().processed() == 30);
                    await(() -> committed(admin, group) == 30);
                    var snapshot = metrics.snapshot();
                    assertThat(snapshot.failed()).isZero();
                    assertThat(snapshot.processedPayloadBytes()).isEqualTo(30 * 3);
                    assertThat(snapshot.processingTime().samples()).isEqualTo(30);
                    assertThat(snapshot.endToEndLatency().samples()).isEqualTo(30);
                    assertThat(snapshot.endToEndLatency().p95Millis()).isGreaterThan(0);
                    var properties = context.getBean(ConsumerFactory.class).getConfigurationProperties();
                    assertThat(properties.get("max.poll.records").toString()).isEqualTo("2");
                    assertThat(properties.get("enable.auto.commit").toString()).isEqualTo("false");
                    assertThat(properties.get("fetch.max.bytes").toString()).isEqualTo("1048576");
                }
                try (var context = start(topic, group)) {
                    var metrics = context.getBean(ConsumerMetrics.class);
                    await(() -> metrics.snapshot().assignedPartitions().size() == 3);
                    publish(topic, 12, 3);
                    await(() -> committed(admin, group) == 42);
                    assertThat(metrics.snapshot().processed()).isEqualTo(12);
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void malformedRecordStopsConsumptionWithoutCommittingPastIt() throws Exception {
        String topic = "consumer-invalid-it-" + UUID.randomUUID();
        String group = "consumer-invalid-it-" + UUID.randomUUID();
        try (var admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            try {
                try (var producer = producer()) {
                    producer.send(new ProducerRecord<>(topic, 0, "bad", "not-json")).get(15, TimeUnit.SECONDS);
                }
                publish(topic, 1, 1);
                try (var context = start(topic, group)) {
                    var metrics = context.getBean(ConsumerMetrics.class);
                    await(() -> metrics.snapshot().failed() == 1);
                    var listener = context.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("telecom-listener");
                    await(() -> !listener.isRunning());
                    assertThat(metrics.snapshot().processed()).isZero();
                    assertThat(committed(admin, group)).isZero();
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void twoInstancesSharePartitionsAndProcessEachRecordOnceInAFailureFreeRun() throws Exception {
        String topic = "consumer-scale-it-" + UUID.randomUUID();
        String group = "consumer-scale-it-" + UUID.randomUUID();
        try (var admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(20, TimeUnit.SECONDS);
            try {
                try (var first = start(topic, group); var second = start(topic, group)) {
                    var one = first.getBean(ConsumerMetrics.class);
                    var two = second.getBean(ConsumerMetrics.class);
                    await(() -> {
                        var a = one.snapshot().assignedPartitions();
                        var b = two.snapshot().assignedPartitions();
                        var union = new HashSet<>(a);
                        union.addAll(b);
                        return !a.isEmpty() && !b.isEmpty() && a.size() + b.size() == 3 && union.size() == 3;
                    });
                    publish(topic, 30, 3);
                    await(() -> committed(admin, group) == 30);
                    assertThat(one.snapshot().processed() + two.snapshot().processed()).isEqualTo(30);
                    assertThat(one.snapshot().processed()).isPositive();
                    assertThat(two.snapshot().processed()).isPositive();
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
            }
        }
    }

    private ConfigurableApplicationContext start(String topic, String group) {
        return SpringApplication.run(ConsumerApplication.class,
                "--consumer.topic=" + topic, "--spring.kafka.consumer.group-id=" + group,
                "--spring.kafka.bootstrap-servers=" + bootstrap,
                "--consumer.cpu-iterations=2", "--consumer.processing-delay-ms=1",
                "--consumer.report-interval-ms=60000", "--spring.kafka.consumer.max-poll-records=2",
                "--spring.kafka.consumer.properties.fetch.max.bytes=1048576", "--debug=false");
    }

    private AdminClient admin() {
        return AdminClient.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", "15000",
                "request.timeout.ms", "10000"));
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class));
    }

    private void publish(String topic, int count, int partitions) throws Exception {
        var mapper = JsonMapper.builder().build();
        String run = UUID.randomUUID().toString();
        try (var producer = producer()) {
            for (int i = 0; i < count; i++) {
                String json = mapper.writeValueAsString(Map.of("eventId", run + ":" + i, "runId", run,
                        "createdAt", System.currentTimeMillis(), "payloadSizeBytes", 3, "payload", "abc",
                        "workloadPhase", "burst", "workloadElapsedNanos", i * 1000000L));
                producer.send(new ProducerRecord<>(topic, i % partitions, "subscriber-" + i, json))
                        .get(15, TimeUnit.SECONDS);
            }
        }
    }

    private long committed(AdminClient admin, String group) {
        try {
            return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS)
                    .values().stream().filter(java.util.Objects::nonNull).mapToLong(value -> value.offset()).sum();
        } catch (Exception error) {
            throw new IllegalStateException("Cannot inspect test consumer offsets", error);
        }
    }

    private void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("consumer test deadline").isLessThan(deadline);
            Thread.sleep(100);
        }
    }
}
