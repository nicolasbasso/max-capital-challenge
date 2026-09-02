package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.KafkaConfigurations;
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
    @Autowired KafkaConfigurations kafkaConfigurations;
    @Value("${app.kafka.execution-reports-topic}") String topic;

    @AfterEach
    void devolverLaIngestaAsuEstadoNormal() {
        TransientFailureInjection.reset();
        registry.start();
        esperarHasta(this::todosCorriendo);
    }

    @Test
    void unFalloTransitorioSeReintentaLasVecesConfiguradasYReciénDespuesFrena() {
        long numericOrderId = 994001L;
        TransientFailureInjection.fallarLasProximas(Integer.MAX_VALUE);

        publicar(numericOrderId, "FIX-994001");

        assertThat(esperarHasta(() -> !todosCorriendo()))
                .as("agotados los reintentos la instancia frena en vez de reintentar para siempre")
                .isTrue();

        assertThat(TransientFailureInjection.invocaciones())
                .as("un intento inicial más los %d reintentos configurados: ni menos, ni para siempre"
                        .formatted(kafkaConfigurations.getRetryMaxAttempts()))
                .isEqualTo(kafkaConfigurations.getRetryMaxAttempts() + 1);

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

    @Test
    void unFalloDesconocidoNoSeReintentaNiUnaVez() {
        long numericOrderId = 994002L;
        TransientFailureInjection.fallarCon(() -> new NullPointerException("bug propio"));
        TransientFailureInjection.fallarLasProximas(Integer.MAX_VALUE);

        publicar(numericOrderId, "FIX-994002");

        assertThat(esperarHasta(() -> !todosCorriendo()))
                .as("un bug propio frena la ingesta")
                .isTrue();

        assertThat(TransientFailureInjection.invocaciones())
                .as("reintentar un bug determinístico da el mismo resultado: se intenta una sola vez")
                .isEqualTo(1);
    }

    private void publicar(long numericOrderId, String fixId) {
        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er(fixId, numericOrderId, OrderStatus.NEW)));
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
