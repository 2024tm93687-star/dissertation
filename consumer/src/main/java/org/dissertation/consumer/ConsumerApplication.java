package org.dissertation.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ConsumerSettings.class)
public class ConsumerApplication {
    public static void main(String[] args) throws InterruptedException {
        var context = SpringApplication.run(ConsumerApplication.class, args);
        var duration = context.getBean(ConsumerSettings.class).runDuration();
        if (duration != null) {
            try {
                Thread.sleep(duration);
            } finally {
                context.close();
            }
        }
    }
}
