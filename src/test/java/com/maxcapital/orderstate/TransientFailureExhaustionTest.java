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
    @Value("${app.kafka.topics.execution-reports}") String topic;

    @AfterEach
    void devolverLaIngestaAsuEstadoNormal() {
        TransientFailureInjection.reset();
        registry.start();
        esperarHasta(this::todosCorriendoConParticiones);
    }

    @Test
    void unFalloTransitorioSeReintentaLasVecesConfiguradasYReciénDespuesFrena() {
        long numericOrderId = Ordenes.nueva();
        TransientFailureInjection.fallarLasProximas(numericOrderId, Integer.MAX_VALUE);

        publicar(numericOrderId, "FIX-%d".formatted(numericOrderId));

        assertThat(esperarHasta(() -> !todosCorriendo()))
                .as("agotados los reintentos la instancia frena en vez de reintentar para siempre")
                .isTrue();

        assertThat(TransientFailureInjection.invocaciones())
                .as("un intento inicial más los %d reintentos configurados: ni menos, ni para siempre"
                        .formatted(kafkaConfigurations.getRetry().getMaxAttempts()))
                .isEqualTo(kafkaConfigurations.getRetry().getMaxAttempts() + 1);

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
        long numericOrderId = Ordenes.nueva();
        TransientFailureInjection.fallarCon(() -> new NullPointerException("bug propio"));
        TransientFailureInjection.fallarLasProximas(numericOrderId, Integer.MAX_VALUE);

        publicar(numericOrderId, "FIX-%d".formatted(numericOrderId));

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

    private boolean todosCorriendoConParticiones() {
        return registry.getListenerContainers().stream().allMatch(container ->
                container.isRunning()
                        && container.getAssignedPartitions() != null
                        && !container.getAssignedPartitions().isEmpty());
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
