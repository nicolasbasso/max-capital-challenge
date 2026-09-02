package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.KafkaConfigurations;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.concurrent.atomic.AtomicBoolean;

@TestConfiguration
public class DeadLetterFailureInjection {

    private static final AtomicBoolean INALCANZABLE = new AtomicBoolean();

    public static void volverlaInalcanzable() {
        INALCANZABLE.set(true);
    }

    public static void reset() {
        INALCANZABLE.set(false);
    }

    @Bean
    @Primary
    DeadLetterPublishingRecoverer deadLetterQuePuedeEstarCaida(KafkaOperations<String, String> kafkaOperations,
                                                               KafkaConfigurations kafkaConfigurations) {
        return new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> new TopicPartition(
                        kafkaConfigurations.getDeadLetterTopic(), record.partition())) {

            @Override
            public void accept(ConsumerRecord<?, ?> record, Exception exception) {
                if (INALCANZABLE.get()) {
                    throw new IllegalStateException("dead letter topic unreachable");
                }
                super.accept(record, exception);
            }
        };
    }
}
