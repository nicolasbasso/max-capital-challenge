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

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryBudgetValidation {

    private static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000L;

    private final ConsumerFactory<?, ?> consumerFactory;
    private final DataSource dataSource;
    private final ExponentialBackOff transientBackOff;
    private final KafkaConfigurations kafkaConfigurations;

    @PostConstruct
    public void retryBudgetMustFitInOnePollInterval() {
        long attempts = kafkaConfigurations.getRetryMaxAttempts() + 1L;
        long attemptCost = connectionTimeout();
        long backOff = totalBackOff();
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

    private long connectionTimeout() {
        if (dataSource instanceof HikariDataSource hikari) {
            return hikari.getConnectionTimeout();
        }
        throw new IllegalStateException("the retry budget is validated against the connection timeout of the pool, "
                + "and assumes HikariCP; found " + dataSource.getClass().getName());
    }

    private long maxPollInterval() {
        Object configured = consumerFactory.getConfigurationProperties()
                .get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        return configured == null ? DEFAULT_MAX_POLL_INTERVAL_MS : Long.parseLong(configured.toString());
    }

    private long totalBackOff() {
        long total = 0;
        BackOffExecution execution = transientBackOff.start();
        for (long interval = execution.nextBackOff();
             interval != BackOffExecution.STOP;
             interval = execution.nextBackOff()) {
            total += interval;
        }
        return total;
    }
}
