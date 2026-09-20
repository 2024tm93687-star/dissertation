package org.dissertation.producer;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.TestConfiguration;
import static org.assertj.core.api.Assertions.assertThat;

class WorkloadPropertiesTest {
    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties(WorkloadProperties.class)
    static class TestConfig { }

    @Test
    void bindsDefaultsAndCommandLineStyleOverrides() {
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class)
                .withPropertyValues("workload.event-count=12", "workload.payload-size-bytes=128")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var config = context.getBean(WorkloadProperties.class);
                    assertThat(config.eventCount()).isEqualTo(12);
                    assertThat(config.payloadSizeBytes()).isEqualTo(128);
                    assertThat(config.ratePerSecond()).isEqualTo(100);
                    assertThat(config.runId()).isEmpty();
                });
    }

    @Test
    void rejectsInvalidSettingsBeforeRunning() {
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class)
                .withPropertyValues("workload.rate-per-second=0")
                .run(context -> assertThat(context).hasFailed());
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var invalid = new WorkloadProperties("bad topic", 0, 0L, 262145, 42, 0, "bad id!",
                    WorkloadProperties.WorkloadPattern.STEADY, null, null, null, null, null);
            assertThat(factory.getValidator().validate(invalid)).hasSize(6);
        }
    }

    @Test
    void rejectsIncompleteBurstConfiguration() {
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class)
                .withPropertyValues("workload.pattern=burst")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void defaultsToCountButDurationHasNoImplicitCountLimit() {
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(WorkloadProperties.class).effectiveEventCount()).isEqualTo(1000);
        });
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class)
                .withPropertyValues("workload.duration=5s").run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(WorkloadProperties.class).eventCount()).isNull();
                });
    }

    @Test
    void acceptsCompleteRampAndBurstConfigurations() {
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class)
                .withPropertyValues("workload.pattern=ramp", "workload.duration=5s", "workload.peak-rate-per-second=200")
                .run(context -> assertThat(context).hasNotFailed());
        new ApplicationContextRunner().withUserConfiguration(TestConfig.class)
                .withPropertyValues("workload.pattern=burst", "workload.duration=4s", "workload.peak-rate-per-second=200",
                        "workload.burst-start=1s", "workload.burst-duration=1s", "workload.quiet-duration=1s")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void rejectsAmbiguousLimitsInvalidDurationsAndIrrelevantOptions() {
        String[][] invalid = {
                {"workload.duration=5s", "workload.event-count=100"},
                {"workload.duration=0s"}, {"workload.duration=-1s"}, {"workload.duration=2h"},
                {"workload.pattern=unknown"}, {"workload.typo=1"},
                {"workload.peak-rate-per-second=200"},
                {"workload.pattern=ramp", "workload.duration=5s", "workload.peak-rate-per-second=50"},
                {"workload.pattern=burst", "workload.duration=4s", "workload.peak-rate-per-second=200",
                        "workload.burst-start=2s", "workload.burst-duration=2s", "workload.quiet-duration=1s"},
                {"workload.pattern=burst", "workload.duration=4s", "workload.peak-rate-per-second=200",
                        "workload.burst-start=1s", "workload.burst-duration=0s"},
                {"workload.pattern=burst", "workload.duration=4s", "workload.peak-rate-per-second=200",
                        "workload.burst-start=-1s", "workload.burst-duration=1s"},
                {"workload.pattern=burst", "workload.duration=4s", "workload.peak-rate-per-second=200",
                        "workload.burst-start=1s", "workload.burst-duration=1s", "workload.quiet-duration=-1s"}
        };
        for (String[] properties : invalid) {
            new ApplicationContextRunner().withUserConfiguration(TestConfig.class).withPropertyValues(properties)
                    .run(context -> assertThat(context).hasFailed());
        }
    }
}
