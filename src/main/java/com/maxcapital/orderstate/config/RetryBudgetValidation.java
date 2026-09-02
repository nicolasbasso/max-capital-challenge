package com.maxcapital.orderstate.config;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

import javax.sql.DataSource;
import java.sql.SQLException;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryBudgetValidation {

    private static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000L;

    private final ConsumerFactory<?, ?> consumerFactory;
    private final DataSource dataSource;
    private final ExponentialBackOff transientBackOff;

    @PostConstruct
    public void retryBudgetMustFitInOnePollInterval() {
        Long attemptCost = connectionTimeout();
        if (attemptCost == null) {
            log.warn("the retry budget assumes HikariCP to know how long one attempt can take; "
                    + "found {}, skipping the check", dataSource.getClass().getName());
            return;
        }

        BackOff plannedRetries = plannedRetries();
        long attempts = plannedRetries.retries + 1L;
        long backOff = plannedRetries.total;
        long worstCase = attempts * attemptCost + backOff;
        long maxPollInterval = maxPollInterval();

        if (worstCase >= maxPollInterval) {
            throw new IllegalStateException(("retrying a transient failure can block the consumer for %dms "
                    + "(%d attempts of up to %dms waiting for a connection, plus %dms of backoff), "
                    + "which reaches max.poll.interval.ms=%dms: the group would evict this consumer "
                    + "mid-processing. Lower app.kafka.retry-max-attempts or "
                    + "spring.datasource.hikari.connection-timeout, or raise max.poll.interval.ms.")
                    .formatted(worstCase, attempts, attemptCost, backOff, maxPollInterval));
        }

        log.info("retry budget: worst case {}ms against max.poll.interval.ms={}ms", worstCase, maxPollInterval);
    }

    private Long connectionTimeout() {
        try {
            if (dataSource.isWrapperFor(HikariDataSource.class)) {
                return dataSource.unwrap(HikariDataSource.class).getConnectionTimeout();
            }
        } catch (SQLException unwrapFailed) {
            log.warn("could not unwrap the data source to read its connection timeout", unwrapFailed);
        }
        return dataSource instanceof HikariDataSource hikari ? hikari.getConnectionTimeout() : null;
    }

    private long maxPollInterval() {
        Object configured = consumerFactory.getConfigurationProperties()
                .get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        return configured == null ? DEFAULT_MAX_POLL_INTERVAL_MS : Long.parseLong(configured.toString());
    }

    private BackOff plannedRetries() {
        long total = 0;
        int retries = 0;
        BackOffExecution execution = transientBackOff.start();
        for (long interval = execution.nextBackOff();
             interval != BackOffExecution.STOP;
             interval = execution.nextBackOff()) {
            total += interval;
            retries++;
        }
        return new BackOff(retries, total);
    }

    private record BackOff(int retries, long total) {
    }
}
