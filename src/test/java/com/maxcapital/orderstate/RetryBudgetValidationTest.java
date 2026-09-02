package com.maxcapital.orderstate;

import com.maxcapital.orderstate.config.RetryBudgetValidation;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.backoff.ExponentialBackOff;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RetryBudgetValidationTest {

    @Test
    void conLosValoresElegidosElPeorCasoEntraEnUnIntervaloDePoll() {
        assertThatCode(() -> validar(pool(30_000), backOff(3), 300_000))
                .as("4 intentos de 30s más el backoff dan 123,5s contra 300s")
                .doesNotThrowAnyException();
    }

    @Test
    void masReintentosDeLaCuentaImpidenQueElServicioArranque() {
        assertThatThrownBy(() -> validar(pool(30_000), backOff(8), 300_000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("315500")
                .hasMessageContaining("max.poll.interval.ms=300000");
    }

    @Test
    void unConnectionTimeoutGrandeTambienLoImpide() {
        assertThatThrownBy(() -> validar(pool(90_000), backOff(3), 300_000))
                .as("el presupuesto no depende sólo de la cantidad de intentos, "
                        + "sino de cuánto puede tardar cada uno")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unPollIntervalMasCortoQueElPresupuestoTambienLoImpide() {
        assertThatThrownBy(() -> validar(pool(30_000), backOff(3), 60_000))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unDataSourceEnvueltoNoImpideArrancar() {
        DataSource envuelto = new LazyConnectionDataSourceProxy(pool(30_000));

        assertThatCode(() -> validar(envuelto, backOff(3), 300_000))
                .as("el pool sigue siendo Hikari aunque venga detrás de un proxy: "
                        + "negarse a arrancar sería un falso positivo")
                .doesNotThrowAnyException();
    }

    @Test
    void losIntentosSalenDelBackoffRealYNoDeUnaPropertyQuePuedeDiscrepar() {
        assertThatCode(() -> validar(pool(30_000), backOff(0), 100_000))
                .as("con el backoff sin reintentos hay un solo intento de 30s: "
                        + "leer la cantidad de otro lado daría 120s y rechazaría un arranque válido")
                .doesNotThrowAnyException();
    }

    @Test
    void unPoolQueNoEsHikariNoRompeElArranque() {
        assertThatCode(() -> validar(mock(DataSource.class), backOff(3), 300_000))
                .as("sin saber cuánto puede tardar un intento no se puede validar, "
                        + "pero eso no es motivo para no arrancar")
                .doesNotThrowAnyException();
    }

    private static HikariDataSource pool(long connectionTimeout) {
        HikariDataSource hikari = new HikariDataSource();
        hikari.setConnectionTimeout(connectionTimeout);
        return hikari;
    }

    private static ExponentialBackOff backOff(int reintentos) {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2);
        backOff.setMaxInterval(10_000);
        backOff.setMaxAttempts(reintentos);
        return backOff;
    }

    private static void validar(DataSource dataSource, ExponentialBackOff backOff, long maxPollInterval) {
        ConsumerFactory<?, ?> consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.getConfigurationProperties()).thenReturn(
                Map.of(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval));

        new RetryBudgetValidation(consumerFactory, dataSource, backOff)
                .retryBudgetMustFitInOnePollInterval();
    }
}
