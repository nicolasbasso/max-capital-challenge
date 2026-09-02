package com.maxcapital.orderstate;

import com.maxcapital.orderstate.service.ExecutionReportService;
import com.maxcapital.orderstate.service.impl.ExecutionReportServiceImpl;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.concurrent.atomic.AtomicInteger;

@TestConfiguration
public class TransientFailureInjection {

    private static final AtomicInteger FALLOS_PENDIENTES = new AtomicInteger();

    public static void fallarLasProximas(int veces) {
        FALLOS_PENDIENTES.set(veces);
    }

    public static int fallosPendientes() {
        return FALLOS_PENDIENTES.get();
    }

    public static void reset() {
        FALLOS_PENDIENTES.set(0);
    }

    @Bean
    @Primary
    ExecutionReportService executionReportServiceConFallosInyectados(ExecutionReportServiceImpl real) {
        return (report, rawPayload) -> {
            if (FALLOS_PENDIENTES.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new DataAccessResourceFailureException("base no disponible");
            }
            real.apply(report, rawPayload);
        };
    }
}
