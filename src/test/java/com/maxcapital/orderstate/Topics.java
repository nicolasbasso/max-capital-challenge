package com.maxcapital.orderstate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class Topics {

    private Topics() {
    }

    static List<ConsumerRecord<String, String>> leerTodo(String bootstrapServers, String topic,
                                                        Duration durante) {
        List<ConsumerRecord<String, String>> leidos = new ArrayList<>();
        try (Consumer<String, String> consumer = nuevoConsumidor(bootstrapServers, topic)) {
            long limite = System.currentTimeMillis() + durante.toMillis();
            while (System.currentTimeMillis() < limite) {
                consumer.poll(Duration.ofMillis(300)).forEach(leidos::add);
            }
        }
        return leidos;
    }

    private static Consumer<String, String> nuevoConsumidor(String bootstrapServers, String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                bootstrapServers, "probe-" + UUID.randomUUID(), "true");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        consumer.subscribe(List.of(topic));
        return consumer;
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
