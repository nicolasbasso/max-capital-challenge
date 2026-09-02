package com.maxcapital.orderstate;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.OrderRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;

class TransientFailurePauseTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired OrderRepository orders;
    @Value("${app.kafka.execution-reports-topic}") String topic;

    @AfterEach
    void devolverElConsumidorAsuEstadoNormal() {
        TransientFailureInjection.reset();
        registry.getListenerContainers().forEach(container -> {
            if (container.isPauseRequested()) {
                container.resume();
            }
        });
    }

    @Test
    void mientrasReintentaUnFalloTransitorioElConsumidorQuedaPausadoYNoBloqueadoEnElPoll() {
        long numericOrderId = 70101L;
        TransientFailureInjection.fallarLasProximas(2);

        ExecutionReportMessage er = ExecutionReports.er("FIX-70101", numericOrderId, OrderStatus.NEW);
        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(er));

        assertThat(huboPausa())
                .as("el backoff se toma pausando el container: si durmiera el hilo, el consumidor dejaría "
                        + "de pollear y el grupo lo daría por muerto en medio del procesamiento")
                .isTrue();

        assertThat(esperarQueSeAplique(numericOrderId))
                .as("pasado el fallo transitorio el ER se aplica igual, sin perderse")
                .isTrue();
        assertThat(TransientFailureInjection.fallosPendientes()).isZero();
    }

    private boolean huboPausa() {
        return esperarHasta(() -> {
            for (MessageListenerContainer container : registry.getListenerContainers()) {
                if (container.isPauseRequested()) {
                    return true;
                }
            }
            return false;
        });
    }

    private boolean esperarQueSeAplique(long numericOrderId) {
        return esperarHasta(() -> orders.findById(numericOrderId).isPresent());
    }

    private static boolean esperarHasta(java.util.function.BooleanSupplier condicion) {
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            if (condicion.getAsBoolean()) {
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
