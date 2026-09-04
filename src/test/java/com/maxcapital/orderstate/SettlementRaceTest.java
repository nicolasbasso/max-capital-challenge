package com.maxcapital.orderstate;

import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.service.ExecutionReportService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementRaceTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired OrderRepository orders;
    @Autowired ExecutionReportService executionReportService;
    @Autowired TransactionTemplate transactionTemplate;
    @PersistenceContext EntityManager entityManager;
    @Value("${app.kafka.topics.execution-reports}") String topic;

    @Test
    void unErQueEsperaAlBarridoNoPuedeBorrarLaMarcaQueElBarridoYaConfirmo() throws Exception {
        long numericOrderId = Ordenes.nueva();
        completar(numericOrderId);

        CountDownLatch marcada = new CountDownLatch(1);
        CountDownLatch commiteá = new CountDownLatch(1);

        Thread barrido = new Thread(() -> transactionTemplate.executeWithoutResult(status -> {
            Order tomada = entityManager.find(Order.class, numericOrderId, LockModeType.PESSIMISTIC_WRITE);
            tomada.settlementPublished(Instant.now());
            entityManager.flush();
            marcada.countDown();
            esperar(commiteá);
        }));
        barrido.start();
        assertThat(marcada.await(20, TimeUnit.SECONDS)).isTrue();

        Thread erTardio = new Thread(() -> executionReportService.apply(
                ExecutionReports.er("FIX-%d-TARDIO".formatted(numericOrderId), numericOrderId, OrderStatus.PARTIALLY_FILLED, 3000, 1956),
                "{}"));
        erTardio.start();
        Thread.sleep(2000);

        commiteá.countDown();
        barrido.join(20_000);
        erTardio.join(20_000);

        Order resultado = orders.findById(numericOrderId).orElseThrow();
        assertThat(resultado.getStatus())
                .as("el ER tardío congela la orden")
                .isEqualTo(OrderStatus.INCOMPLETE);
        assertThat(resultado.getSettlementPublishedAt())
                .as("el barrido ya publicó y confirmó la marca: un ER que venía leyendo la fila "
                        + "vieja no puede borrarla, o el aviso de cambio no se manda nunca")
                .isNotNull();
    }

    private void completar(long numericOrderId) {
        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-%d-1".formatted(numericOrderId), numericOrderId, OrderStatus.NEW)));
        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-%d-2".formatted(numericOrderId), numericOrderId, OrderStatus.FILLED, 4956, 0)));
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            if (orders.findById(numericOrderId).map(Order::getStatus).orElse(null) == OrderStatus.FILLED) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("la orden no llegó a FILLED");
    }

    private static void esperar(CountDownLatch latch) {
        try {
            if (!latch.await(20, TimeUnit.SECONDS)) {
                throw new IllegalStateException("no llegó la señal para commitear");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
