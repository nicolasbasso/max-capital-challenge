package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.KafkaConfigurations;
import com.maxcapital.orderstate.repository.OrderRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterUnavailableTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired KafkaConfigurations kafkaConfigurations;
    @Autowired OrderRepository orders;
    @Value("${app.kafka.topics.execution-reports}") String topic;

    @AfterEach
    void devolverLaIngestaAsuEstadoNormal() {
        DeadLetterFailureInjection.reset();
        registry.start();
        esperarHasta(this::todosCorriendoConParticiones);
    }

    @Test
    void siNoSePuedePublicarEnLaDeadLetterSeFrenaLaIngestaYElErNoSePierde() {
        long numericOrderId = 996001L;
        DeadLetterFailureInjection.volverlaInalcanzable();

        kafka.send(topic, String.valueOf(numericOrderId), """
                {"fixId": "   ", "numericOrderId": %d, "status": "NEW",
                 "nominalAmounts": 4956, "accumulativeNominalAmount": 0, "leavesNominalAmount": 4956}
                """.formatted(numericOrderId));

        assertThat(esperarHasta(() -> !todosCorriendo()))
                .as("si el único lugar donde podíamos preservar el ER no está, frenar es lo "
                        + "que evita descartarlo en silencio")
                .isTrue();

        assertThat(orders.findById(numericOrderId))
                .as("no se persistió nada")
                .isEmpty();

        DeadLetterFailureInjection.reset();
        registry.start();

        assertThat(Topics.esperar(KAFKA.getBootstrapServers(), kafkaConfigurations.getTopics().getDeadLetter(),
                String.valueOf(numericOrderId), Duration.ofSeconds(45)))
                .as("el offset nunca se commiteó, así que al volver la dead letter el ER se preserva igual")
                .isNotNull();
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
