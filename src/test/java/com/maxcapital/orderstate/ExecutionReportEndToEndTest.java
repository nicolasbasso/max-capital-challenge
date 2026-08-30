package com.maxcapital.orderstate;

import com.maxcapital.orderstate.dto.OrderResponse;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionReportEndToEndTest extends IntegrationTestBase {
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired OrderRepository orders;
    @Autowired ExecutionLedgerRepository ledger;
    @Autowired TestRestTemplate http;
    @Value("${app.kafka.execution-reports-topic}") String topic;

    @Test
    void unNewRecorreElSistemaYQuedaConsultable() {
        long numericOrderId = 13144742L;
        String fixId = "523130930000307";

        kafka.send(topic, String.valueOf(numericOrderId), """
                {
                  "fixId": "%s",
                  "numericOrderId": %d,
                  "status": "NEW",
                  "ticker": "VSCPC",
                  "side": "BUY",
                  "nominalAmounts": 4956,
                  "transactionTime": "2026-07-20T18:08:52.129"
                }
                """.formatted(fixId, numericOrderId));

        esperarHasta(() -> orders.findById(numericOrderId).isPresent());

        var order = orders.findById(numericOrderId).orElseThrow();
        assertThat(order.getStatus().name()).isEqualTo("NEW");
        assertThat(order.getAppliedExecutions())
                .as("un ER aplicado incrementa el contador exactamente una vez")
                .isEqualTo(1);

        var entries = ledger.findByNumericOrderIdOrderByIdAsc(numericOrderId);
        assertThat(entries)
                .as("una entrada de ledger por ER efectivamente aplicado")
                .hasSize(1);
        assertThat(entries.getFirst().getFixId()).isEqualTo(fixId);

        ResponseEntity<OrderResponse> response =
                http.getForEntity("/orders/" + numericOrderId, OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status().name()).isEqualTo("NEW");
        assertThat(response.getBody().appliedExecutions()).isEqualTo(1);
        assertThat(response.getBody().ledger()).hasSize(1);
        assertThat(response.getBody().ledger().getFirst().fixId()).isEqualTo(fixId);
    }

    @Test
    void unaOrdenInexistenteDevuelve404() {
        assertThat(http.getForEntity("/orders/999999999", String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    private void esperarHasta(java.util.function.BooleanSupplier condicion) {
        Instant limite = Instant.now().plus(Duration.ofSeconds(20));
        while (Instant.now().isBefore(limite)) {
            if (condicion.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("el ER no se proceso dentro del tiempo esperado");
    }
}
