package org.dissertation.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class WorkloadProducerTest {
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final WorkloadProperties config = new WorkloadProperties("test", 100000, 3L, 128, 42, 2, "unit",
            WorkloadProperties.WorkloadPattern.STEADY, null, null, null, null, null);

    private void withTopic() {
        when(kafka.partitionsFor("test")).thenReturn(List.of(new PartitionInfo("test", 0, null, null, null)));
    }

    @Test
    void countsAcknowledgementsAndSerializedBytes() throws Exception {
        withTopic();
        when(kafka.send(eq("test"), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        var result = new WorkloadProducer(kafka, config).produce();
        assertThat(result.acknowledged()).isEqualTo(3);
        assertThat(result.serializedValueBytes()).isGreaterThan(3 * 128);
        assertThat(result.eventStartSpanSeconds()).isPositive();
        assertThat(result.eventStartRatePerSecond()).isEqualTo(2 / result.eventStartSpanSeconds());
        assertThat(result.pacingResetCount()).isNotNegative();
        assertThat(result.maxPacingLatenessMillis()).isNotNegative();
        verify(kafka, times(3)).send(eq("test"), anyString(), anyString());
    }

    @Test
    void failedAcknowledgementStopsTheRun() {
        withTopic();
        when(kafka.send(eq("test"), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker failure")));
        assertThatThrownBy(() -> new WorkloadProducer(kafka, config).produce())
                .hasMessageContaining("incomplete");
        verify(kafka, times(1)).send(eq("test"), anyString(), anyString());
    }

    @Test
    void synchronousSendFailureIsNotReportedAsSuccess() {
        withTopic();
        when(kafka.send(eq("test"), anyString(), anyString())).thenThrow(new IllegalStateException("buffer full"));
        assertThatThrownBy(() -> new WorkloadProducer(kafka, config).produce())
                .hasMessageContaining("Send failed");
    }

    @Test
    void failureOnTheFinalSendIsNotReportedAsSuccess() {
        withTopic();
        when(kafka.send(eq("test"), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("last send failed")));
        var single = singleEvent();
        assertThatThrownBy(() -> new WorkloadProducer(kafka, single).produce())
                .hasMessageContaining("incomplete");
    }

    @Test
    void waitsForFinalAcknowledgement() throws Exception {
        withTopic();
        var last = new CompletableFuture<SendResult<String, String>>();
        var sent = new java.util.concurrent.CountDownLatch(1);
        when(kafka.send(eq("test"), anyString(), anyString())).thenAnswer(invocation -> {
            sent.countDown();
            return last;
        });
        var single = singleEvent();
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var result = executor.submit(() -> new WorkloadProducer(kafka, single).produce());
            try {
                assertThat(sent.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThat(result.isDone()).isFalse();
                last.complete(null);
                var summary = result.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(summary.acknowledged()).isEqualTo(1);
                assertThat(summary.eventStartSpanSeconds()).isZero();
                assertThat(summary.eventStartRatePerSecond()).isNull();
            } finally {
                last.completeExceptionally(new IllegalStateException("test cleanup"));
                result.cancel(true);
            }
        }
    }

    private WorkloadProperties singleEvent() {
        return new WorkloadProperties("test", 100, 1L, 128, 42, 1, "unit",
                WorkloadProperties.WorkloadPattern.STEADY, null, null, null, null, null);
    }

    @Test
    void durationExpiresWhileBackpressuredAndStillDrainsTheLastAcknowledgement() throws Exception {
        withTopic();
        var delivery = new CompletableFuture<SendResult<String, String>>();
        var firstSend = new java.util.concurrent.CountDownLatch(1);
        when(kafka.send(eq("test"), anyString(), anyString())).thenAnswer(invocation -> {
            firstSend.countDown();
            return delivery;
        });
        var timed = new WorkloadProperties("test", 100, null, 128, 42, 1, "unit",
                WorkloadProperties.WorkloadPattern.STEADY, java.time.Duration.ofMillis(100), null, null, null, null);
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var result = executor.submit(() -> new WorkloadProducer(kafka, timed).produce());
            try {
                assertThat(firstSend.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> result.get(250, java.util.concurrent.TimeUnit.MILLISECONDS))
                        .isInstanceOf(java.util.concurrent.TimeoutException.class);
                delivery.complete(null);
                var summary = result.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(summary.sent()).isEqualTo(1);
                assertThat(summary.acknowledged()).isEqualTo(1);
                assertThat(summary.elapsedSeconds()).isGreaterThan(0.1);
                assertThat(summary.phases().getFirst().acknowledged()).isEqualTo(1);
                verify(kafka, times(1)).send(eq("test"), anyString(), anyString());
            } finally {
                delivery.completeExceptionally(new IllegalStateException("test cleanup"));
                result.cancel(true);
            }
        }
    }
}
