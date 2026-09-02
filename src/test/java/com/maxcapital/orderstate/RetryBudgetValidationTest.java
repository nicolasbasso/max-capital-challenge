package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.KafkaConfigurations;
import com.maxcapital.orderstate.config.RetryBudgetValidation;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RetryBudgetValidationTest {

    @Test
    void conLosValoresElegidosElPeorCasoEntraEnUnIntervaloDePoll() {
        assertThatCode(() -> validacionCon(3, 30_000, 300_000))
                .as("4 intentos de 30s más el backoff dan 123,5s contra 300s")
                .doesNotThrowAnyException();
    }

    @Test
    void masReintentosDeLaCuentaImpidenQueElServicioArranque() {
        assertThatThrownBy(() -> validacionCon(8, 30_000, 300_000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("315500")
                .hasMessageContaining("max.poll.interval.ms=300000");
    }

    @Test
    void unConnectionTimeoutGrandeTambienLoImpide() {
        assertThatThrownBy(() -> validacionCon(3, 90_000, 300_000))
                .as("el presupuesto no depende sólo de la cantidad de intentos, "
                        + "sino de cuánto puede tardar cada uno")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unPollIntervalMasCortoQueElPresupuestoTambienLoImpide() {
        assertThatThrownBy(() -> validacionCon(3, 30_000, 60_000))
                .isInstanceOf(IllegalStateException.class);
    }

    private static void validacionCon(int reintentos, long connectionTimeout, long maxPollInterval) {
        HikariDataSource dataSource = mock(HikariDataSource.class);
        when(dataSource.getConnectionTimeout()).thenReturn(connectionTimeout);

        ConsumerFactory<?, ?> consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.getConfigurationProperties()).thenReturn(
                Map.of(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval));

        KafkaConfigurations configurations = mock(KafkaConfigurations.class);
        when(configurations.getRetryMaxAttempts()).thenReturn(reintentos);

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2);
        backOff.setMaxInterval(10_000);
        backOff.setMaxAttempts(reintentos);

        new RetryBudgetValidation(consumerFactory, dataSource, backOff, configurations)
                .retryBudgetMustFitInOnePollInterval();
    }
}
