package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.SettlementConfigurations;
import com.maxcapital.orderstate.model.ExecutionAmounts;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.service.SettlementPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementConcurrentSweepTest extends IntegrationTestBase {

    private static final int CUANTAS = 25;

    @Autowired SettlementPublisher settlementPublisher;
    @Autowired OrderRepository orders;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired SettlementConfigurations settlementConfigurations;

    @Test
    void dosBarridosEnParaleloNoPublicanDosVecesLaMismaOrden() throws Exception {
        List<Long> completadas = IntStream.range(0, CUANTAS).mapToObj(i -> Ordenes.nueva()).toList();
        transactionTemplate.executeWithoutResult(status -> completadas.forEach(this::completar));

        CountDownLatch largada = new CountDownLatch(1);
        List<Thread> barridos = List.of(barrido(largada), barrido(largada));
        barridos.forEach(Thread::start);
        largada.countDown();
        for (Thread barrido : barridos) {
            barrido.join(60_000);
        }

        Map<String, Long> porOrden = Topics.leerTodo(KAFKA.getBootstrapServers(),
                        settlementConfigurations.getTopic(), Duration.ofSeconds(4)).stream()
                .map(registro -> registro.key())
                .filter(key -> completadas.contains(Long.valueOf(key)))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertThat(porOrden)
                .as("las dos instancias barren cada segundo: sin el lock, las dos encuentran la "
                        + "misma fila pendiente y downstream recibe el settlement dos veces")
                .hasSize(CUANTAS)
                .allSatisfy((orden, veces) -> assertThat(veces).isEqualTo(1L));
    }

    private Thread barrido(CountDownLatch largada) {
        return new Thread(() -> {
            try {
                largada.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            settlementPublisher.publishPendingSettlements();
        });
    }

    private void completar(long numericOrderId) {
        Order orden = Order.opening(numericOrderId);
        orden.applyExecution(OrderStatus.FILLED, ExecutionAmounts.of(
                BigDecimal.valueOf(4956), BigDecimal.valueOf(4956), BigDecimal.ZERO));
        orders.save(orden);
    }
}
