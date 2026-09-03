package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.KafkaConfigurations;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaConfigurations kafkaConfigurations;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired OrderRepository orders;
    @Autowired ExecutionLedgerRepository ledger;
    @Value("${app.kafka.topics.execution-reports}") String topic;

    @Test
    void unErSinIdentidadVaALaDeadLetterYNoBloqueaLoQueVieneDetras() {
        long numericOrderId = 70001L;

        unErQueRompeElContrato(numericOrderId, """
                {"fixId": "   ", "numericOrderId": %d, "status": "NEW",
                 "nominalAmounts": 4956, "accumulativeNominalAmount": 0, "leavesNominalAmount": 4956}
                """.formatted(numericOrderId));
    }

    @Test
    void unStatusQueNoExisteEnElEnumVaALaDeadLetterYNoBloqueaLoQueVieneDetras() {
        long numericOrderId = 70002L;

        unErQueRompeElContrato(numericOrderId, """
                {"fixId": "FIX-%d-BOGUS", "numericOrderId": %d, "status": "BOGUS",
                 "nominalAmounts": 4956, "accumulativeNominalAmount": 0, "leavesNominalAmount": 4956}
                """.formatted(numericOrderId, numericOrderId));
    }

    private void unErQueRompeElContrato(long numericOrderId, String payloadRoto) {
        String key = String.valueOf(numericOrderId);

        kafka.send(topic, key, payloadRoto);
        kafka.send(topic, key, ExecutionReports.raw(
                ExecutionReports.er("FIX-" + numericOrderId, numericOrderId, OrderStatus.NEW)));

        assertThat(esperarEnDeadLetter(numericOrderId))
                .as("un ER fuera de contrato se preserva en la dead letter en vez de descartarse")
                .isNotNull();

        assertThat(esperarQueSeAplique(numericOrderId))
                .as("el ER válido publicado detrás, en la misma partición, se procesa igual: "
                        + "una falla de contrato no bloquea el flujo de esa orden")
                .isTrue();

        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(numericOrderId))
                .as("sólo entra al ledger el ER que sí se pudo aplicar")
                .hasSize(1);

        assertThat(registry.getListenerContainers())
                .as("ninguna instancia frena por una falla de contrato")
                .allSatisfy(container -> assertThat(container.isRunning()).isTrue());
    }

    private boolean esperarQueSeAplique(long numericOrderId) {
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            if (orders.findById(numericOrderId).isPresent()) {
                return true;
            }
            dormir();
        }
        return false;
    }

    private ConsumerRecord<String, String> esperarEnDeadLetter(long numericOrderId) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                KAFKA.getBootstrapServers(), "dlt-probe-" + UUID.randomUUID(), "true");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer()) {

            consumer.subscribe(List.of(kafkaConfigurations.getTopics().getDeadLetter()));
            long limite = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < limite) {
                ConsumerRecords<String, String> lote = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> registro : lote) {
                    if (registro.value() != null && registro.value().contains(String.valueOf(numericOrderId))) {
                        return registro;
                    }
                }
            }
        }
        return null;
    }

    private static void dormir() {
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
