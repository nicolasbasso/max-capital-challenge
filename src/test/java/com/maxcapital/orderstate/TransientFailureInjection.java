package com.maxcapital.orderstate;

import com.maxcapital.orderstate.service.ExecutionReportService;
import com.maxcapital.orderstate.service.impl.ExecutionReportServiceImpl;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@TestConfiguration
public class TransientFailureInjection {

    private static final long NINGUNA = -1L;

    private static final Supplier<RuntimeException> BASE_CAIDA =
            () -> new DataAccessResourceFailureException("base no disponible");

    private static final AtomicLong ORDEN_OBJETIVO = new AtomicLong(NINGUNA);
    private static final AtomicInteger FALLOS_PENDIENTES = new AtomicInteger();
    private static final AtomicInteger INVOCACIONES = new AtomicInteger();

    private static volatile Supplier<RuntimeException> falla = BASE_CAIDA;

    public static void fallarLasProximas(long numericOrderId, int veces) {
        INVOCACIONES.set(0);
        FALLOS_PENDIENTES.set(veces);
        ORDEN_OBJETIVO.set(numericOrderId);
    }

    public static void fallarCon(Supplier<RuntimeException> excepcion) {
        falla = excepcion;
    }

    public static int invocaciones() {
        return INVOCACIONES.get();
    }

    public static void reset() {
        ORDEN_OBJETIVO.set(NINGUNA);
        FALLOS_PENDIENTES.set(0);
        INVOCACIONES.set(0);
        falla = BASE_CAIDA;
    }

    @Bean
    @Primary
    ExecutionReportService executionReportServiceConFallosInyectados(ExecutionReportServiceImpl real) {
        return (report, rawPayload) -> {
            if (report.numericOrderId() != null && report.numericOrderId() == ORDEN_OBJETIVO.get()) {
                INVOCACIONES.incrementAndGet();
                if (FALLOS_PENDIENTES.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                    throw falla.get();
                }
            }
            real.apply(report, rawPayload);
        };
    }
}
