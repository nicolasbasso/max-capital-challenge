package com.maxcapital.orderstate;

import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.OrderRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class TransientFailureExhaustionTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired OrderRepository orders;
    @Value("${app.kafka.execution-reports-topic}") String topic;

    @AfterEach
    void devolverLaIngestaAsuEstadoNormal() {
        TransientFailureInjection.reset();
        registry.start();
        esperarHasta(this::todosCorriendo);
    }

    @Test
    void unFalloTransitorioQueNoCedeFrenaLaIngestaYNoPierdeElEr() {
        long numericOrderId = 994001L;
        TransientFailureInjection.fallarLasProximas(Integer.MAX_VALUE);

        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er("FIX-994001", numericOrderId, OrderStatus.NEW)));

        assertThat(esperarHasta(() -> !todosCorriendo()))
                .as("agotados los reintentos la instancia frena la ingesta en vez de "
                        + "reintentar para siempre o descartar el ER")
                .isTrue();

        assertThat(orders.findById(numericOrderId))
                .as("nada se aplicó: el fallo era del entorno, no del mensaje")
                .isEmpty();

        TransientFailureInjection.reset();
        registry.start();

        assertThat(esperarHasta(() -> orders.findById(numericOrderId).isPresent()))
                .as("el offset no se commiteó, así que al volver la ingesta el ER se aplica: "
                        + "frenar no pierde nada")
                .isTrue();
    }

    private boolean todosCorriendo() {
        return registry.getListenerContainers().stream().allMatch(MessageListenerContainer::isRunning);
    }

    private static boolean esperarHasta(BooleanSupplier condicion) {
        long limite = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < limite) {
            if (condicion.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        return false;
    }
}
