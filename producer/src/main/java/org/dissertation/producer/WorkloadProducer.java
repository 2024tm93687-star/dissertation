package org.dissertation.producer;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
public class WorkloadProducer {
    private static final Logger LOG = LoggerFactory.getLogger(WorkloadProducer.class);
    private final KafkaTemplate<String, String> kafka;
    private final WorkloadProperties config;

    public WorkloadProducer(KafkaTemplate<String, String> kafka, WorkloadProperties config) {
        this.kafka = kafka;
        this.config = config;
    }

    public RunSummary produce() throws InterruptedException {
        String runId = config.runId().isBlank() ? UUID.randomUUID().toString() : config.runId();
        var events = new EventFactory(runId, config.payloadSizeBytes(), config.seed());
        var mapper = JsonMapper.builder().build();
        var pending = new Semaphore(config.maxPendingSends());
        var acknowledged = new AtomicLong();
        var bytes = new AtomicLong();
        var failure = new AtomicReference<Throwable>();
        var timeline = config.duration() == null ? null : new WorkloadTimeline(config);
        var metrics = timeline == null ? null : new TimelineMetrics(timeline);

        // Resolve metadata before starting the measured workload interval.
        if (kafka.partitionsFor(config.topic()).isEmpty()) {
            throw new IllegalStateException("Topic has no partitions: " + config.topic());
        }
        LOG.info("Starting run={} topic={} pattern={} count={} duration={} rate={} payloadBytes={}", runId,
                config.topic(), config.pattern(), timeline == null ? config.effectiveEventCount() : null,
                config.duration(), config.ratePerSecond(), config.payloadSizeBytes());
        long startedAt = System.currentTimeMillis();
        long startNanos = System.nanoTime();
        var pacer = new SteadyRatePacer(config.ratePerSecond());
        var timelinePacer = timeline == null ? null : new TimelinePacer(timeline, startNanos);
        long firstEventStart = 0;
        long lastEventStart = 0;
        long sent = 0;

        while (timeline != null || sent < config.effectiveEventCount()) {
            long waitNanos = TimeUnit.SECONDS.toNanos(35);
            if (timeline != null) {
                long remaining = timeline.durationNanos() - (System.nanoTime() - startNanos);
                if (remaining <= 0) {
                    break;
                }
                waitNanos = Math.min(waitNanos, remaining);
            }
            if (!pending.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
                if (timeline != null && System.nanoTime() - startNanos >= timeline.durationNanos()) {
                    break;
                }
                throw new IllegalStateException("Timed out waiting for Kafka send capacity");
            }
            checkFailure(failure, acknowledged);
            long eventElapsed;
            String phase = "steady";
            if (timelinePacer == null) {
                eventElapsed = pacer.awaitSlot() - startNanos;
            } else {
                if (timelinePacer.awaitSlot() < 0) {
                    pending.release();
                    break;
                }
                // Recheck after waking: no event generation starts in quiet or expired phases.
                eventElapsed = System.nanoTime() - startNanos;
                int index = timeline.phaseIndexAt(eventElapsed);
                if (index < 0 || timeline.phases().get(index).rateAt(eventElapsed) == 0) {
                    pending.release();
                    continue;
                }
                phase = timeline.phases().get(index).name();
                checkFailure(failure, acknowledged);
            }
            var event = events.create(sent, System.currentTimeMillis(), phase, eventElapsed);
            String json = mapper.writeValueAsString(event);
            int valueBytes = json.getBytes(StandardCharsets.UTF_8).length;
            try {
                var delivery = kafka.send(config.topic(), event.subscriberId(), json);
                Runnable recordAck = metrics == null ? () -> { } : metrics.recordSend(eventElapsed);
                delivery.whenComplete((result, error) -> {
                    if (error == null) {
                        acknowledged.incrementAndGet();
                        bytes.addAndGet(valueBytes);
                        recordAck.run();
                    } else {
                        failure.compareAndSet(null, error);
                    }
                    pending.release();
                });
                if (sent == 0) {
                    firstEventStart = eventElapsed;
                }
                lastEventStart = eventElapsed;
                sent++;
            } catch (RuntimeException error) {
                pending.release();
                throw new IllegalStateException("Send failed after " + acknowledged.get() + " acknowledgements", error);
            }
        }
        if (!pending.tryAcquire(config.maxPendingSends(), 35, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for final Kafka acknowledgements");
        }
        checkFailure(failure, acknowledged);
        double elapsed = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        double eventStartSpan = (lastEventStart - firstEventStart) / 1_000_000_000.0;
        Double eventStartRate = sent > 1 ? (sent - 1) / eventStartSpan : null;
        return new RunSummary(runId, config.topic(), config.pattern().name().toLowerCase(Locale.ROOT),
                config.seed(), config.ratePerSecond(),
                config.payloadSizeBytes(), timeline == null ? config.effectiveEventCount() : null,
                acknowledged.get(), bytes.get(),
                startedAt, System.currentTimeMillis(), elapsed, acknowledged.get() / elapsed,
                eventStartSpan, eventStartRate,
                timelinePacer == null ? pacer.resetCount() : timelinePacer.resetCount(),
                (timelinePacer == null ? pacer.maxLatenessNanos() : timelinePacer.maxLatenessNanos()) / 1_000_000.0,
                timeline == null ? null : timeline.durationNanos() / 1e9, sent,
                metrics == null ? List.of() : metrics.phases(), metrics == null ? List.of() : metrics.windows());
    }

    private static void checkFailure(AtomicReference<Throwable> failure, AtomicLong acknowledged) {
        if (failure.get() != null) {
            throw new IllegalStateException("Kafka delivery failed after " + acknowledged.get()
                    + " acknowledgements; this run is incomplete", failure.get());
        }
    }
}
