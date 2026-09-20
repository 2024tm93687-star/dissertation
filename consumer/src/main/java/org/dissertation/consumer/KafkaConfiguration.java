package org.dissertation.consumer;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, ConsumerMetrics metrics, ConsumerSettings settings) {
        if (Boolean.parseBoolean(String.valueOf(consumerFactory.getConfigurationProperties()
                .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)))) {
            throw new IllegalArgumentException("Automatic commits must remain disabled for measured processing");
        }
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(settings.concurrency());
        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
        var container = factory.getContainerProperties();
        container.setAckMode(ContainerProperties.AckMode.BATCH);
        container.setSyncCommits(true);
        container.setSyncCommitTimeout(Duration.ofSeconds(15));
        container.setPollTimeout(1000);
        container.setShutdownTimeout(30000);
        container.setMissingTopicsFatal(true);
        container.setConsumerRebalanceListener(metrics);
        return factory;
    }
}
