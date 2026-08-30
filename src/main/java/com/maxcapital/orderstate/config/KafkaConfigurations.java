package com.maxcapital.orderstate.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaConfigurations {
    @NotBlank
    private String executionReportsTopic;

    @Positive
    private int partitions;

    @Positive
    private int replicas;

    @Bean
    NewTopic executionReportsNewTopic() {
        return TopicBuilder.name(executionReportsTopic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }
}
