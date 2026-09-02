package com.maxcapital.orderstate;

import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.config.KafkaConfigurations;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class StrictContractTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired OrderRepository orders;
    @Autowired ExecutionLedgerRepository ledger;
    @Autowired KafkaConfigurations kafkaConfigurations;
    @Value("${app.kafka.execution-reports-topic}") String topic;

    @Test
    void unNumericOrderIdDecimalNoSeTruncaSobreUnaOrdenSana() {
        long numericOrderId = 993001L;

        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-993001", numericOrderId, OrderStatus.NEW)));
        assertThat(esperarElFixId(numericOrderId, "FIX-993001")).isTrue();

        kafka.send(topic, String.valueOf(numericOrderId), """
                {"fixId":"FIX-993001-DECIMAL","numericOrderId":%d.9,"status":"PARTIALLY_FILLED",
                 "nominalAmounts":4956,"accumulativeNominalAmount":2000,"leavesNominalAmount":2956}
                """.formatted(numericOrderId));

        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-993001-2", numericOrderId, OrderStatus.PARTIALLY_FILLED, 2000, 2956)));
        assertThat(esperarElFixId(numericOrderId, "FIX-993001-2"))
                .as("el ER válido de atrás se procesa: el decimal no bloqueó la partición")
                .isTrue();

        Order orden = orders.findById(numericOrderId).orElseThrow();
        assertThat(orden.getAppliedExecutions())
                .as("un id decimal no puede truncarse y aplicarse sobre la orden %d, que es sana"
                        .formatted(numericOrderId))
                .isEqualTo(2);
        assertThat(enLaDeadLetter("FIX-993001-DECIMAL"))
                .as("rechazarlo no es descartarlo: el ER queda preservado")
                .isNotNull();
    }

    @Test
    void unStatusNumericoNoSeInterpretaComoLaPosicionDelEnum() {
        long numericOrderId = 993002L;

        kafka.send(topic, String.valueOf(numericOrderId), """
                {"fixId":"FIX-993002","numericOrderId":%d,"status":0,
                 "nominalAmounts":4956,"accumulativeNominalAmount":0,"leavesNominalAmount":4956}
                """.formatted(numericOrderId));

        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-993002-OK", numericOrderId, OrderStatus.NEW)));
        assertThat(esperarElFixId(numericOrderId, "FIX-993002-OK")).isTrue();

        Order orden = orders.findById(numericOrderId).orElseThrow();
        assertThat(orden.getAppliedExecutions())
                .as("status 0 no es NEW: un número no es un estado del contrato")
                .isEqualTo(1);
        assertThat(orden.getStatus()).isEqualTo(OrderStatus.NEW);
        assertThat(enLaDeadLetter("\"status\":0")).isNotNull();
    }

    @Test
    void unNumericOrderIdComoStringNoSeAcepta() {
        long numericOrderId = 993003L;

        kafka.send(topic, String.valueOf(numericOrderId), """
                {"fixId":"FIX-993003","numericOrderId":"%d","status":"NEW",
                 "nominalAmounts":4956,"accumulativeNominalAmount":0,"leavesNominalAmount":4956}
                """.formatted(numericOrderId));

        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-993003-OK", numericOrderId, OrderStatus.NEW)));
        assertThat(esperarElFixId(numericOrderId, "FIX-993003-OK")).isTrue();

        assertThat(orders.findById(numericOrderId).orElseThrow().getAppliedExecutions())
                .as("el id llega como número o no llega: el string no entra por coerción")
                .isEqualTo(1);
        assertThat(enLaDeadLetter("\"numericOrderId\":\"993003\"")).isNotNull();
    }

    private org.apache.kafka.clients.consumer.ConsumerRecord<String, String> enLaDeadLetter(String contiene) {
        return DeadLetters.esperar(KAFKA.getBootstrapServers(), kafkaConfigurations.getDeadLetterTopic(),
                contiene, java.time.Duration.ofSeconds(30));
    }

    private boolean esperarElFixId(long numericOrderId, String fixId) {
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            boolean aplicado = ledger.findByNumericOrderIdOrderByIdAsc(numericOrderId).stream()
                    .anyMatch(entrada -> fixId.equals(entrada.getFixId()));
            if (aplicado) {
                return true;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        return false;
    }
}
