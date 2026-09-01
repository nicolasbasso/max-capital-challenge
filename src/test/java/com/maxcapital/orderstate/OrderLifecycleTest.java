package com.maxcapital.orderstate;

import com.maxcapital.orderstate.dto.LedgerEntryResponse;
import com.maxcapital.orderstate.dto.OrderResponse;
import com.maxcapital.orderstate.dto.QuarantinedEntryResponse;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.model.ExecutionLedgerEntry;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.model.QuarantineReason;
import com.maxcapital.orderstate.model.QuarantinedExecutionReport;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.repository.QuarantineRepository;
import com.maxcapital.orderstate.service.ExecutionReportService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;

import java.math.BigDecimal;
import java.util.List;

import static com.maxcapital.orderstate.ExecutionReports.er;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderLifecycleTest extends IntegrationTestBase {

    @Autowired ExecutionReportService service;
    @Autowired OrderRepository orders;
    @Autowired ExecutionLedgerRepository ledger;
    @Autowired QuarantineRepository quarantine;
    @Autowired TestRestTemplate http;

    @Test
    void elCicloCompletoDejaContadorYLedgerAlineados() {
        long id = 20001L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));
        aplicar(er("F-3", id, OrderStatus.FILLED, 4956, 0));

        Order order = orden(id);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getAppliedExecutions()).isEqualTo(3);
        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id))
                .as("el contador tiene que igualar la cantidad de entradas del ledger")
                .hasSize(order.getAppliedExecutions());
        assertThat(quarantine.findByNumericOrderIdOrderByIdAsc(id)).isEmpty();
    }

    @Test
    void losCamposCuantitativosSonSnapshotsNoDeltas() {
        long id = 20002L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));
        aplicar(er("F-3", id, OrderStatus.PARTIALLY_FILLED, 3000, 1956));

        Order order = orden(id);
        assertThat(order.getAmounts().getAccumulativeNominalAmount())
                .as("si se sumaran como deltas daria 4200, no 3000")
                .isEqualByComparingTo(BigDecimal.valueOf(3000));
        assertThat(order.getAmounts().getLeavesNominalAmount()).isEqualByComparingTo(BigDecimal.valueOf(1956));
        assertThat(order.getAmounts().getNominalAmount()).isEqualByComparingTo(BigDecimal.valueOf(4956));
    }

    @Test
    void elLedgerSeDevuelveEnElOrdenEnQueSeAplicaronLosEr() {
        long id = 20003L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));
        aplicar(er("F-3", id, OrderStatus.PARTIALLY_FILLED, 3000, 1956));
        aplicar(er("F-4", id, OrderStatus.FILLED, 4956, 0));

        List<ExecutionLedgerEntry> entries = ledger.findByNumericOrderIdOrderByIdAsc(id);

        assertThat(entries).extracting(ExecutionLedgerEntry::getFixId)
                .containsExactly("F-1", "F-2", "F-3", "F-4");
        assertThat(entries).extracting(ExecutionLedgerEntry::getId)
                .as("la clave autoincremental refleja el orden de insercion")
                .isSorted();
        assertThat(entries).extracting(e -> e.getAmounts().getAccumulativeNominalAmount())
                .as("accumulativeNominalAmount es monotono no decreciente dentro de una orden")
                .isSorted();
    }

    @Test
    void laReentregaDelFilledEsUnNoOpYNoCongelaLaOrden() {
        long id = 20004L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        var filled = er("F-2", id, OrderStatus.FILLED, 4956, 0);
        aplicar(filled);

        assertThatThrownBy(() -> aplicar(filled))
                .as("una reentrega es un duplicado conocido, no un error")
                .isInstanceOf(DuplicateExecutionReportException.class);

        Order order = orden(id);
        assertThat(order.getStatus())
                .as("si la terminalidad se validara antes que la dedup, esto seria INCOMPLETE")
                .isEqualTo(OrderStatus.FILLED);
        assertThat(order.getAppliedExecutions()).isEqualTo(2);
        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id)).hasSize(2);
        assertThat(quarantine.findByNumericOrderIdOrderByIdAsc(id)).isEmpty();
    }

    @Test
    void unErPosteriorAUnTerminalCongelaLaOrdenYSePreservaEnCuarentena() {
        long id = 20005L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.FILLED, 4956, 0));

        aplicar(er("F-3", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));

        Order order = orden(id);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.INCOMPLETE);
        assertThat(order.getAppliedExecutions())
                .as("un ER rechazado no mueve el contador")
                .isEqualTo(2);
        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id))
                .as("el ledger es una entrada por ER efectivamente aplicado")
                .hasSize(2);

        List<QuarantinedExecutionReport> cuarentenados = quarantine.findByNumericOrderIdOrderByIdAsc(id);
        assertThat(cuarentenados).hasSize(1);
        assertThat(cuarentenados.getFirst().getFixId()).isEqualTo("F-3");
        assertThat(cuarentenados.getFirst().getIncomingStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(cuarentenados.getFirst().getOrderStatusAtRejection()).isEqualTo(OrderStatus.FILLED);
        assertThat(cuarentenados.getFirst().getReason()).isEqualTo(QuarantineReason.STATE_TRANSITION_REJECTED);
        assertThat(cuarentenados.getFirst().getRecordedAt()).isNotNull();
        assertThat(cuarentenados.getFirst().getRawPayload())
                .as("el ER se preserva completo: el DTO ignora estos campos y sin el crudo se perderian")
                .contains("secondaryTradeId")
                .contains("operationNumber")
                .contains("avgPrice")
                .contains("transactionTime");
    }

    @Test
    void laReentregaDeUnErCuarentenadoSeDetectaPorIdentidadYNoDuplicaLaCuarentena() {
        long id = 20006L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.FILLED, 4956, 0));
        var extraviado = er("F-3", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756);
        aplicar(extraviado);

        assertThatThrownBy(() -> aplicar(extraviado))
                .as("la constraint de cuarentena reconoce el fixId: proteccion por identidad")
                .isInstanceOf(DuplicateExecutionReportException.class);

        assertThat(quarantine.findByNumericOrderIdOrderByIdAsc(id)).hasSize(1);
        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id)).hasSize(2);
        assertThat(orden(id).getAppliedExecutions()).isEqualTo(2);
        assertThat(orden(id).getStatus()).isEqualTo(OrderStatus.INCOMPLETE);
    }

    @Test
    void unaOrdenSoloSeAbreConUnNewYSinElNaceCongelada() {
        long id = 20007L;

        aplicar(er("F-1", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));

        Order order = orden(id);
        assertThat(order.getStatus())
                .as("existe para poder consultarla: sin la fila el GET devolvia 404 y la orden se perdia")
                .isEqualTo(OrderStatus.INCOMPLETE);
        assertThat(order.getAppliedExecutions()).isZero();
        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id)).isEmpty();

        List<QuarantinedExecutionReport> cuarentenados = quarantine.findByNumericOrderIdOrderByIdAsc(id);
        assertThat(cuarentenados).hasSize(1);
        assertThat(cuarentenados.getFirst().getOrderStatusAtRejection())
                .as("no habia estado previo que registrar: es la fila (no existe) de la tabla de D-005")
                .isNull();
    }

    @Test
    void unSegundoNewCongelaLaOrdenEnVezDeHacerlaRetroceder() {
        long id = 20008L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));

        aplicar(er("F-3", id, OrderStatus.NEW, 0, 4956));

        Order order = orden(id);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.INCOMPLETE);
        assertThat(order.getAppliedExecutions()).isEqualTo(2);
        assertThat(order.getAmounts().getAccumulativeNominalAmount())
                .as("el ER rechazado no puede pisar las cantidades ya aplicadas")
                .isEqualByComparingTo(BigDecimal.valueOf(1200));
        assertThat(quarantine.findByNumericOrderIdOrderByIdAsc(id)).hasSize(1);
    }

    @Test
    void unaOrdenCongeladaNoAceptaNadaMas() {
        long id = 20009L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.NEW, 0, 4956));

        aplicar(er("F-3", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));

        assertThat(orden(id).getStatus()).isEqualTo(OrderStatus.INCOMPLETE);
        assertThat(orden(id).getAppliedExecutions()).isEqualTo(1);
        assertThat(quarantine.findByNumericOrderIdOrderByIdAsc(id))
                .as("cada ER rechazado se preserva por separado")
                .hasSize(2);
    }

    @Test
    void elLedgerTambienGuardaElErCompleto() {
        long id = 20011L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));

        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id).getFirst().getRawPayload())
                .as("reconstruir el ciclo de vida necesita el ER entero, no solo los campos que mapeamos")
                .contains("secondaryTradeId")
                .contains("avgPrice")
                .contains("transactionTime");
    }

    @Test
    void unaOrdenSeCancelaDespuesDeUnParcialAunqueElRemanenteQuedeEnCero() {
        long id = 20015L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.PARTIALLY_FILLED, 3756, 1200));

        aplicar(er("F-3", id, OrderStatus.CANCELLED, 3756, 0));

        Order order = orden(id);
        assertThat(order.getStatus())
                .as("cancelar despues de un parcial es una transicion legal de D-005")
                .isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getAppliedExecutions()).isEqualTo(3);
        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(id)).hasSize(3);
        assertThat(quarantine.findByNumericOrderIdOrderByIdAsc(id))
                .as("un remanente en cero al cancelar no es motivo para congelar la orden")
                .isEmpty();
    }

    @Test
    void elGetDevuelveElLedgerAplicadoYLaCuarentenaEnLaMismaRespuesta() {
        long id = 20010L;
        aplicar(er("F-1", id, OrderStatus.NEW, 0, 4956));
        aplicar(er("F-2", id, OrderStatus.FILLED, 4956, 0));
        aplicar(er("F-3", id, OrderStatus.PARTIALLY_FILLED, 1200, 3756));

        String cuerpo = http.getForObject("/orders/" + id, String.class);
        assertThat(cuerpo)
                .as("el ER preservado viaja completo en la respuesta, no sus campos mapeados")
                .contains("secondaryTradeId")
                .contains("STATE_TRANSITION_REJECTED");

        OrderResponse response = http.getForObject("/orders/" + id, OrderResponse.class);

        assertThat(response.status()).isEqualTo(OrderStatus.INCOMPLETE);
        assertThat(response.appliedExecutions()).isEqualTo(2);
        assertThat(response.ledger()).extracting(LedgerEntryResponse::fixId).containsExactly("F-1", "F-2");
        assertThat(response.quarantine())
                .as("el ER que rompio la orden viaja en la misma respuesta que la orden rota")
                .extracting(QuarantinedEntryResponse::fixId)
                .containsExactly("F-3");
        assertThat(response.accumulativeNominalAmount()).isEqualByComparingTo(BigDecimal.valueOf(4956));
    }

    private void aplicar(com.maxcapital.orderstate.dto.ExecutionReportMessage report) {
        service.apply(report, ExecutionReports.raw(report));
    }

    private Order orden(long id) {
        return orders.findById(id).orElseThrow();
    }
}
