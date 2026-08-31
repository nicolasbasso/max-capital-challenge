package com.maxcapital.orderstate;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.handler.ExecutionReportConsumer;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.service.ExecutionReportService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ExecutionReportConsumerTest {

    private ExecutionReportService service;
    private Acknowledgment acknowledgment;
    private ExecutionReportConsumer consumer;

    private static final ExecutionReportMessage REPORT =
            new ExecutionReportMessage("FIX-1", 13144742L, OrderStatus.NEW);

    @BeforeEach
    void setUp() {
        service = mock(ExecutionReportService.class);
        acknowledgment = mock(Acknowledgment.class);
        consumer = new ExecutionReportConsumer(service);
    }

    @Test
    void unErValidoSeAplicaYAvanzaElOffset() {
        consumer.onExecutionReport(REPORT, 2, 100L, acknowledgment);

        verify(service).apply(REPORT);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void unDuplicadoNoEsUnaFallaYTambienAvanzaElOffset() {
        doThrow(new DuplicateExecutionReportException(13144742L, "FIX-1"))
                .when(service).apply(any(ExecutionReportMessage.class));

        consumer.onExecutionReport(REPORT, 2, 100L, acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void siLaAplicacionFallaDeVerdadElOffsetNoAvanza() {
        doThrow(new IllegalStateException("base caida"))
                .when(service).apply(any(ExecutionReportMessage.class));

        try {
            consumer.onExecutionReport(REPORT, 2, 100L, acknowledgment);
        } catch (IllegalStateException expected) {
            // se deja propagar para que el error handler la tome
        }

        verify(acknowledgment, never()).acknowledge();
    }
}
