package com.maxcapital.orderstate.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicsConfiguration {

    private final KafkaConfigurations kafkaConfigurations;

    @Bean
    NewTopic executionReportsNewTopic() {
        return TopicBuilder.name(kafkaConfigurations.getExecutionReportsTopic())
                .partitions(kafkaConfigurations.getPartitions())
                .replicas(kafkaConfigurations.getReplicas())
                .build();
    }

    @Bean
    NewTopic deadLetterNewTopic() {
        return TopicBuilder.name(kafkaConfigurations.getDeadLetterTopic())
                .partitions(kafkaConfigurations.getPartitions())
                .replicas(kafkaConfigurations.getReplicas())
                .build();
    }
}
