package com.maxcapital.orderstate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class DeadLetters {

    private DeadLetters() {
    }

    static ConsumerRecord<String, String> esperar(String bootstrapServers, String topic, String contiene,
                                                  Duration tiempoMaximo) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                bootstrapServers, "dlt-probe-" + UUID.randomUUID(), "true");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer()) {

            consumer.subscribe(List.of(topic));
            long limite = System.currentTimeMillis() + tiempoMaximo.toMillis();
            while (System.currentTimeMillis() < limite) {
                ConsumerRecords<String, String> lote = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> registro : lote) {
                    if (registro.value() != null && registro.value().contains(contiene)) {
                        return registro;
                    }
                }
            }
        }
        return null;
    }
}
