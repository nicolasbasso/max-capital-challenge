package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.KafkaConfigurations;
import com.maxcapital.orderstate.handler.ExecutionReportFailureRecoverer;
import com.maxcapital.orderstate.handler.ExecutionReportFailureRecoverer.FailureKind;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.orm.jpa.JpaSystemException;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionReportFailureRecovererTest {

    private DeadLetterPublishingRecoverer deadLetter;
    private KafkaListenerEndpointRegistry registry;
    private ExecutionReportFailureRecoverer recoverer;

    @BeforeEach
    void setUp() {
        deadLetter = mock(DeadLetterPublishingRecoverer.class);
        registry = mock(KafkaListenerEndpointRegistry.class);

        @SuppressWarnings("unchecked")
        ObjectProvider<KafkaListenerEndpointRegistry> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(registry);

        KafkaConfigurations configurations = mock(KafkaConfigurations.class);
        when(configurations.getRetryMaxAttempts()).thenReturn(3);

        recoverer = new ExecutionReportFailureRecoverer(deadLetter, provider, configurations);
    }

    @Test
    void unStatusQueNoExisteEnElEnumEsFallaDeContratoYVaALaDeadLetter() {
        Exception real = new ListenerExecutionFailedException("listener failed",
                new ConversionException("Failed to convert from JSON",
                        new IllegalArgumentException("No enum constant OrderStatus.BOGUS")));

        assertThat(ExecutionReportFailureRecoverer.classify(real)).isEqualTo(FailureKind.CONTRACT);

        recoverer.accept(unRegistro(), real);

        verify(deadLetter).accept(any(), any());
        verify(registry, never()).stop();
    }

    @Test
    void laClasificacionRecorreLaCadenaEnteraYNoSoloLasPuntas() {
        Exception envuelta = new IllegalStateException("wrapper de afuera",
                new RuntimeException("wrapper del medio",
                        new ConversionException("Failed to convert from JSON",
                                new IllegalArgumentException("causa mas profunda, que no dice nada"))));

        assertThat(ExecutionReportFailureRecoverer.classify(envuelta))
                .as("ni la excepción de afuera ni la causa más profunda son de contrato: "
                        + "la que lo dice está en el medio")
                .isEqualTo(FailureKind.CONTRACT);
    }

    @Test
    void laBaseCaidaEsTransitoriaYAlAgotarseFrenaLaIngesta() {
        Exception baseCaida = new ListenerExecutionFailedException("listener failed",
                new DataAccessResourceFailureException("Connection is not available"));

        assertThat(ExecutionReportFailureRecoverer.classify(baseCaida)).isEqualTo(FailureKind.TRANSIENT);

        recoverer.accept(unRegistro(), baseCaida);

        verify(registry).stop();
        verify(deadLetter, never()).accept(any(), any());
    }

    @Test
    void unBugPropioNoEsTransitorioNiDeContratoYFrenaSinMandarloALaDeadLetter() {
        Exception bug = new ListenerExecutionFailedException("listener failed",
                new NullPointerException("bug propio"));

        assertThat(ExecutionReportFailureRecoverer.classify(bug))
                .as("reintentar un bug determinístico da el mismo resultado; "
                        + "mandarlo a la dead letter descarta un mensaje que puede ser válido")
                .isEqualTo(FailureKind.UNKNOWN);

        recoverer.accept(unRegistro(), bug);

        verify(registry).stop();
        verify(deadLetter, never()).accept(any(), any());
    }

    @Test
    void siLaDeadLetterNoEstaDisponibleSeFrenaLaIngesta() {
        doThrow(new IllegalStateException("dead letter topic unreachable"))
                .when(deadLetter).accept(any(), any());

        recoverer.accept(unRegistro(), new ConversionException("Failed to convert from JSON",
                new IllegalArgumentException("da igual")));

        verify(registry)
                .stop();
    }

    @Test
    void unCorteDeConexionEnMedioDeLaTransaccionEsTransitorioAunqueElRollbackLoTape() {
        Exception corteDeRed = new ListenerExecutionFailedException("listener failed",
                new JpaSystemException(new org.hibernate.TransactionException("could not roll back",
                        new SQLException("connection reset by peer", "08006"))));

        assertThat(ExecutionReportFailureRecoverer.classify(corteDeRed))
                .as("ninguna clase de la cadena está en la lista: lo que dice que es de conexión "
                        + "es el SQLState 08 del driver")
                .isEqualTo(FailureKind.TRANSIENT);

        recoverer.accept(unRegistro(), corteDeRed);

        verify(registry).stop();
        verify(deadLetter, never()).accept(any(), any());
    }

    @Test
    void laClasificacionMiraTambienLasCausasSuprimidas() {
        Exception rollbackFallido = new IllegalStateException("el rollback reemplazó la causa original");
        rollbackFallido.addSuppressed(new SQLException("connection reset by peer", "08006"));

        assertThat(ExecutionReportFailureRecoverer.classify(
                new ListenerExecutionFailedException("listener failed", rollbackFallido)))
                .as("cuando el rollback tapa la excepción primaria, la original queda como suprimida")
                .isEqualTo(FailureKind.TRANSIENT);
    }

    private static ConsumerRecord<String, String> unRegistro() {
        return new ConsumerRecord<>("execution-reports", 0, 0L, "70001", "{}");
    }
}
