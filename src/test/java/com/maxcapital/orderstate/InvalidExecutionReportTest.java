package com.maxcapital.orderstate;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.service.ExecutionReportService;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InvalidExecutionReportTest extends IntegrationTestBase {

    @Autowired ExecutionReportService executionReportService;
    @Autowired ExecutionLedgerRepository ledger;
    @Autowired Validator validator;

    @Test
    void unFixIdMasLargoQueLaColumnaNoSeConfundeConUnDuplicado() {
        String demasiadoLargo = "X".repeat(100);
        var report = new ExecutionReportMessage(demasiadoLargo, 90001L, OrderStatus.NEW);

        assertThatThrownBy(() -> executionReportService.apply(report))
                .as("solo la violacion de uq_execution_ledger_order_fix es un duplicado")
                .isInstanceOf(DataIntegrityViolationException.class)
                .isNotInstanceOf(DuplicateExecutionReportException.class);

        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(90001L)).isEmpty();
    }

    @Test
    void elMismoErDosVecesSiEsUnDuplicado() {
        var report = new ExecutionReportMessage("FIX-DUP-1", 90002L, OrderStatus.NEW);
        executionReportService.apply(report);

        assertThatThrownBy(() -> executionReportService.apply(report))
                .isInstanceOf(DuplicateExecutionReportException.class);

        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(90002L)).hasSize(1);
    }

    @Test
    void unFixIdAusenteVacioODemasiadoLargoNoPasaValidacion() {
        assertThat(validator.validate(new ExecutionReportMessage(null, 1L, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage("", 1L, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage("   ", 1L, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage("X".repeat(65), 1L, OrderStatus.NEW))).isNotEmpty();

        assertThat(validator.validate(new ExecutionReportMessage("FIX-OK", 1L, OrderStatus.NEW))).isEmpty();
    }

    @Test
    void unNumericOrderIdOUnStatusAusenteNoPasaValidacion() {
        assertThat(validator.validate(new ExecutionReportMessage("FIX-OK", null, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage("FIX-OK", 1L, null))).isNotEmpty();
    }
}
