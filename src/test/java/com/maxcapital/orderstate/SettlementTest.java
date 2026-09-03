package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.SettlementConfigurations;
import com.maxcapital.orderstate.config.SettlementSchedulerConfiguration;
import com.maxcapital.orderstate.dto.OrderResponse;
import com.maxcapital.orderstate.service.OrderQueryService;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.service.SettlementPublisher;
import com.maxcapital.orderstate.service.impl.SettlementPublisherImpl;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementTest extends IntegrationTestBase {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired SettlementPublisher settlementPublisher;
    @Autowired OrderQueryService orderQueryService;
    @Autowired OrderRepository orders;
    @Autowired ExecutionLedgerRepository ledger;
    @Autowired SettlementConfigurations settlementConfigurations;
    @Autowired ScheduledAnnotationBeanPostProcessor scheduledPostProcessor;
    @Value("${app.kafka.topics.execution-reports}") String topic;

    @Test
    void unaOrdenQueCompletaProduceUnSettlementYSoloUno() {
        long numericOrderId = 970001L;
        completar(numericOrderId);

        settlementPublisher.publishPendingSettlements();

        assertThat(eventosDe(numericOrderId))
                .as("la orden completó: se le avisa a downstream una sola vez")
                .containsExactly("ORDER_SETTLED");
        assertThat(orders.findById(numericOrderId).orElseThrow().getSettlementPublishedAt()).isNotNull();
        assertThat(consultar(numericOrderId).settlementPublishedAt())
                .as("la conversación con downstream se consulta por el GET, no se busca en logs")
                .isNotNull();

        settlementPublisher.publishPendingSettlements();

        assertThat(eventosDe(numericOrderId))
                .as("el segundo barrido ya no la encuentra pendiente")
                .containsExactly("ORDER_SETTLED");
    }

    @Test
    void unaOrdenCanceladaNoProduceSettlement() {
        long numericOrderId = 970002L;
        enviar(numericOrderId, "FIX-%d-1".formatted(numericOrderId), OrderStatus.NEW, 0, 4956);
        enviar(numericOrderId, "FIX-%d-2".formatted(numericOrderId), OrderStatus.CANCELLED, 0, 4956);
        esperarElFixId(numericOrderId, "FIX-%d-2".formatted(numericOrderId));

        settlementPublisher.publishPendingSettlements();
        settlementPublisher.publishPendingIncompleteNotices();

        assertThat(eventosDe(numericOrderId))
                .as("R16: una orden cancelada no settlea")
                .isEmpty();
    }

    @Test
    void unErTardioDespuesDePublicarProduceElAvisoYEnEseOrden() {
        long numericOrderId = 970003L;
        completar(numericOrderId);
        settlementPublisher.publishPendingSettlements();

        enviar(numericOrderId, "FIX-%d-TARDIO".formatted(numericOrderId), OrderStatus.PARTIALLY_FILLED, 1234, 3722);
        esperarQueQuede(numericOrderId, OrderStatus.INCOMPLETE);

        settlementPublisher.publishPendingIncompleteNotices();

        assertThat(eventosDe(numericOrderId))
                .as("misma clave, mismo topic: el aviso llega después del settlement sin "
                        + "inventar nada de secuencia")
                .containsExactly("ORDER_SETTLED", "ORDER_MARKED_INCOMPLETE");
        assertThat(consultar(numericOrderId).markedIncompleteNotifiedAt()).isNotNull();
    }

    @Test
    void unErTardioAntesDePublicarSuprimeElSettlement() {
        long numericOrderId = 970004L;
        completar(numericOrderId);

        enviar(numericOrderId, "FIX-%d-TARDIO".formatted(numericOrderId), OrderStatus.PARTIALLY_FILLED, 1234, 3722);
        esperarQueQuede(numericOrderId, OrderStatus.INCOMPLETE);

        settlementPublisher.publishPendingSettlements();
        settlementPublisher.publishPendingIncompleteNotices();

        assertThat(eventosDe(numericOrderId))
                .as("cuando pudimos hablar ya sabíamos que estaba contradicha: no se afirma "
                        + "algo que se sabe falso")
                .isEmpty();
    }

    @Test
    void unaOrdenIncompletaQueNuncaCompletoNoProduceNada() {
        long numericOrderId = 970005L;
        enviar(numericOrderId, "FIX-%d-1".formatted(numericOrderId), OrderStatus.NEW, 0, 4956);
        enviar(numericOrderId, "FIX-%d-2".formatted(numericOrderId), OrderStatus.NEW, 0, 4956);
        esperarQueQuede(numericOrderId, OrderStatus.INCOMPLETE);

        settlementPublisher.publishPendingSettlements();
        settlementPublisher.publishPendingIncompleteNotices();

        assertThat(eventosDe(numericOrderId))
                .as("nunca completó, así que no hay nada que corregir ni que anunciar")
                .isEmpty();
    }

    @Test
    void laReentregaDelErQueCompletoNoProduceUnSegundoSettlement() {
        long numericOrderId = 970006L;
        completar(numericOrderId);
        settlementPublisher.publishPendingSettlements();

        enviar(numericOrderId, "FIX-%d-3".formatted(numericOrderId), OrderStatus.FILLED, 4956, 0);
        settlementPublisher.publishPendingSettlements();

        assertThat(eventosDe(numericOrderId))
                .as("D-002 deduplica el ER, así que no hay un segundo completado que anunciar")
                .containsExactly("ORDER_SETTLED");
        assertThat(orders.findById(numericOrderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.FILLED);
    }

    private OrderResponse consultar(long numericOrderId) {
        return orderQueryService.getByNumericOrderId(numericOrderId);
    }

    @Test
    void losDosMensajesVanKeyeadosPorNumericOrderId() {
        long numericOrderId = 970007L;
        completar(numericOrderId);
        settlementPublisher.publishPendingSettlements();

        enviar(numericOrderId, "FIX-%d-TARDIO".formatted(numericOrderId), OrderStatus.PARTIALLY_FILLED, 1234, 3722);
        esperarQueQuede(numericOrderId, OrderStatus.INCOMPLETE);
        settlementPublisher.publishPendingIncompleteNotices();

        assertThat(registrosDe(numericOrderId))
                .hasSize(2)
                .allSatisfy(registro -> assertThat(registro.key())
                        .as("la key es lo que manda los dos mensajes de una orden a la misma "
                                + "partición, y de ahí sale que el aviso llegue después del settlement")
                        .isEqualTo(String.valueOf(numericOrderId)));
    }

    @Test
    void losDosBarridosCorrenSolosPorqueEstanAgendados() {
        String publicador = SettlementPublisherImpl.class.getName();
        Set<String> agendados = scheduledPostProcessor.getScheduledTasks().stream()
                .map(tarea -> tarea.getTask().getRunnable().toString())
                .filter(metodo -> metodo.startsWith(publicador))
                .collect(Collectors.toSet());

        assertThat(agendados)
                .as("sin @Scheduled los barridos sólo corren si alguien los llama a mano, "
                        + "y en producción no hay nadie que lo haga")
                .containsExactlyInAnyOrder(publicador + ".publishPendingSettlements",
                        publicador + ".publishPendingIncompleteNotices");
    }

    @Test
    void losBarridosNoCorrenEnElSchedulerDelBackoffDeKafka() throws Exception {
        for (String metodo : List.of("publishPendingSettlements", "publishPendingIncompleteNotices")) {
            Scheduled agendado = SettlementPublisherImpl.class.getMethod(metodo).getAnnotation(Scheduled.class);
            assertThat(agendado.scheduler())
                    .as("sin declarar el scheduler, los barridos toman el único TaskScheduler del "
                            + "contexto, que es el del backoff de Kafka: comparten un hilo con la "
                            + "tarea que despausa el container y estiran el presupuesto de reintentos")
                    .isEqualTo(SettlementSchedulerConfiguration.SETTLEMENT_SCHEDULER);
        }
        assertThat(scheduledPostProcessor.getScheduledTasks()).isNotEmpty();
    }

    private void completar(long numericOrderId) {
        enviar(numericOrderId, "FIX-%d-1".formatted(numericOrderId), OrderStatus.NEW, 0, 4956);
        enviar(numericOrderId, "FIX-%d-2".formatted(numericOrderId), OrderStatus.PARTIALLY_FILLED, 2000, 2956);
        enviar(numericOrderId, "FIX-%d-3".formatted(numericOrderId), OrderStatus.FILLED, 4956, 0);
        esperarElFixId(numericOrderId, "FIX-%d-3".formatted(numericOrderId));
    }

    private void enviar(long numericOrderId, String fixId, OrderStatus status, long acumulado, long remanente) {
        kafka.send(topic, String.valueOf(numericOrderId), ExecutionReports.raw(
                ExecutionReports.er(fixId, numericOrderId, status, acumulado, remanente)));
    }

    private List<String> eventosDe(long numericOrderId) {
        return registrosDe(numericOrderId).stream().map(SettlementTest::tipoDe).toList();
    }

    private List<ConsumerRecord<String, String>> registrosDe(long numericOrderId) {
        return Topics.leerTodo(KAFKA.getBootstrapServers(), settlementConfigurations.getTopic(),
                        Duration.ofSeconds(2)).stream()
                .filter(registro -> registro.value().contains("\"numericOrderId\":" + numericOrderId))
                .toList();
    }

    private static String tipoDe(ConsumerRecord<String, String> registro) {
        return registro.value().replaceAll(".*\"type\"\\s*:\\s*\"([A-Z_]+)\".*", "$1");
    }

    private void esperarElFixId(long numericOrderId, String fixId) {
        esperar(() -> ledger.findByNumericOrderIdOrderByIdAsc(numericOrderId).stream()
                .anyMatch(entrada -> fixId.equals(entrada.getFixId())));
    }

    private void esperarQueQuede(long numericOrderId, OrderStatus status) {
        esperar(() -> orders.findById(numericOrderId).map(Order::getStatus).orElse(null) == status);
    }

    private static void esperar(java.util.function.BooleanSupplier condicion) {
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            if (condicion.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("la condición no se cumplió en 30s");
    }
}
