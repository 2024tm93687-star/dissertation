package org.dissertation.producer;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

@SpringBootApplication
@EnableConfigurationProperties(WorkloadProperties.class)
public class ProducerApplication {
    public static void main(String[] args) {
        try (var context = SpringApplication.run(ProducerApplication.class, args)) {
            // A workload is finite; closing the context also closes Kafka connections.
        }
    }

    @Bean
    ApplicationRunner workloadRunner(WorkloadProducer producer) {
        return args -> System.out.println("PRODUCER_SUMMARY "
                + JsonMapper.builder().build().writeValueAsString(producer.produce()));
    }
}
