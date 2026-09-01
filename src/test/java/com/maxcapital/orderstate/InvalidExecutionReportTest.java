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

import java.math.BigDecimal;

import static com.maxcapital.orderstate.ExecutionReports.er;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InvalidExecutionReportTest extends IntegrationTestBase {

    @Autowired ExecutionReportService executionReportService;
    @Autowired ExecutionLedgerRepository ledger;
    @Autowired Validator validator;

    @Test
    void unFixIdMasLargoQueLaColumnaNoSeConfundeConUnDuplicado() {
        String demasiadoLargo = "X".repeat(100);
        var report = er(demasiadoLargo, 90001L, OrderStatus.NEW);

        assertThatThrownBy(() -> executionReportService.apply(report, ExecutionReports.raw(report)))
                .as("solo la violacion de uq_execution_ledger_order_fix es un duplicado")
                .isInstanceOf(DataIntegrityViolationException.class)
                .isNotInstanceOf(DuplicateExecutionReportException.class);

        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(90001L)).isEmpty();
    }

    @Test
    void elMismoErDosVecesSiEsUnDuplicado() {
        var report = er("FIX-DUP-1", 90002L, OrderStatus.NEW);
        executionReportService.apply(report, ExecutionReports.raw(report));

        assertThatThrownBy(() -> executionReportService.apply(report, ExecutionReports.raw(report)))
                .isInstanceOf(DuplicateExecutionReportException.class);

        assertThat(ledger.findByNumericOrderIdOrderByIdAsc(90002L)).hasSize(1);
    }

    @Test
    void unFixIdAusenteVacioODemasiadoLargoNoPasaValidacion() {
        assertThat(validator.validate(er(null, 1L, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(er("", 1L, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(er("   ", 1L, OrderStatus.NEW))).isNotEmpty();
        assertThat(validator.validate(er("X".repeat(65), 1L, OrderStatus.NEW))).isNotEmpty();

        assertThat(validator.validate(er("FIX-OK", 1L, OrderStatus.NEW))).isEmpty();
    }

    @Test
    void unNumericOrderIdOUnStatusAusenteNoPasaValidacion() {
        assertThat(validator.validate(new ExecutionReportMessage(
                "FIX-OK", null, OrderStatus.NEW, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage(
                "FIX-OK", 1L, null, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE))).isNotEmpty();
    }

    @Test
    void unaCantidadAusenteONegativaNoPasaValidacion() {
        assertThat(validator.validate(new ExecutionReportMessage(
                "FIX-OK", 1L, OrderStatus.NEW, null, BigDecimal.ZERO, BigDecimal.ONE))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage(
                "FIX-OK", 1L, OrderStatus.NEW, BigDecimal.ONE, null, BigDecimal.ONE))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage(
                "FIX-OK", 1L, OrderStatus.NEW, BigDecimal.ONE, BigDecimal.ZERO, null))).isNotEmpty();
        assertThat(validator.validate(new ExecutionReportMessage(
                "FIX-OK", 1L, OrderStatus.NEW, BigDecimal.ONE, BigDecimal.valueOf(-1), BigDecimal.ONE))).isNotEmpty();
    }
}
