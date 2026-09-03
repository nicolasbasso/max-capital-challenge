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
    private final SettlementConfigurations settlementConfigurations;

    @Bean
    NewTopic executionReportsNewTopic() {
        return TopicBuilder.name(kafkaConfigurations.getTopics().getExecutionReports())
                .partitions(kafkaConfigurations.getPartitions())
                .replicas(kafkaConfigurations.getReplicas())
                .build();
    }

    @Bean
    NewTopic settlementsNewTopic() {
        return TopicBuilder.name(settlementConfigurations.getTopic())
                .partitions(kafkaConfigurations.getPartitions())
                .replicas(kafkaConfigurations.getReplicas())
                .build();
    }

    @Bean
    NewTopic deadLetterNewTopic() {
        return TopicBuilder.name(kafkaConfigurations.getTopics().getDeadLetter())
                .partitions(kafkaConfigurations.getPartitions())
                .replicas(kafkaConfigurations.getReplicas())
                .build();
    }
}
