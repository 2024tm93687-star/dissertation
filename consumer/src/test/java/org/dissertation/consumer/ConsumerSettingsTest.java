package org.dissertation.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.*;

class ConsumerSettingsTest {
    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ConsumerSettings.class)
    static class Config { }

    @Test
    void bindsDefaultsAndOverrides() {
        new ApplicationContextRunner().withUserConfiguration(Config.class)
                .withPropertyValues("consumer.cpu-iterations=50", "consumer.processing-delay-ms=2", "consumer.run-duration=10s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var settings = context.getBean(ConsumerSettings.class);
                    assertThat(settings.cpuIterations()).isEqualTo(50);
                    assertThat(settings.processingDelayMs()).isEqualTo(2);
                    assertThat(settings.concurrency()).isEqualTo(1);
                    assertThat(settings.runDuration().toSeconds()).isEqualTo(10);
                });
    }

    @Test
    void rejectsInvalidAndUnknownOptions() {
        for (String property : new String[] {"consumer.cpu-iterations=-1", "consumer.concurrency=0",
                "consumer.processing-delay-ms=-1", "consumer.report-interval-ms=0",
                "consumer.run-duration=0s", "consumer.run-duration=2h", "consumer.typo=1"}) {
            new ApplicationContextRunner().withUserConfiguration(Config.class).withPropertyValues(property)
                    .run(context -> assertThat(context).hasFailed());
        }
    }

    @Test
    void kafkaEnvironmentOverridesDoNotBecomeUnknownConsumerSettings() {
        new ApplicationContextRunner().withUserConfiguration(Config.class)
                .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new org.springframework.core.env.SystemEnvironmentPropertySource("test-environment",
                                java.util.Map.of("CONSUMER_GROUP_ID", "test-group",
                                        "CONSUMER_MAX_POLL_RECORDS", "10", "CONSUMER_CPU_ITERATIONS", "2"))))
                .run(context -> assertThat(context).hasNotFailed());
    }
}
