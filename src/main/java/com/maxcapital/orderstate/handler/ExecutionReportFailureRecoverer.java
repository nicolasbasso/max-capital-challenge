package com.maxcapital.orderstate.handler;

import com.maxcapital.orderstate.config.KafkaConfigurations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLTransientException;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionReportFailureRecoverer implements ConsumerRecordRecoverer {

    public enum FailureKind {
        CONTRACT, TRANSIENT, UNKNOWN
    }

    private static final List<Class<? extends Throwable>> CONTRACT_FAILURES = List.of(
            MethodArgumentNotValidException.class,
            MessageConversionException.class,
            ConversionException.class);

    private static final List<Class<? extends Exception>> TRANSIENT_FAILURES = List.of(
            TransientDataAccessException.class,
            DataAccessResourceFailureException.class,
            CannotCreateTransactionException.class,
            SQLTransientException.class,
            RetriableException.class);

    private final DeadLetterPublishingRecoverer deadLetter;
    private final ObjectProvider<KafkaListenerEndpointRegistry> registry;
    private final KafkaConfigurations kafkaConfigurations;

    public static List<Class<? extends Exception>> transientFailures() {
        return TRANSIENT_FAILURES;
    }

    public static FailureKind classify(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (matches(cause, CONTRACT_FAILURES)) {
                return FailureKind.CONTRACT;
            }
            if (matches(cause, TRANSIENT_FAILURES)) {
                return FailureKind.TRANSIENT;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return FailureKind.UNKNOWN;
    }

    private static boolean matches(Throwable cause, List<? extends Class<? extends Throwable>> types) {
        return types.stream().anyMatch(type -> type.isInstance(cause));
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        switch (classify(exception)) {
            case CONTRACT -> sendToDeadLetter(record, exception);
            case TRANSIENT -> stopIngestion(record, exception,
                    "transient failure did not clear in %d retries".formatted(kafkaConfigurations.getRetryMaxAttempts()));
            case UNKNOWN -> stopIngestion(record, exception,
                    "unexpected failure, not retryable and not a contract breach");
        }
    }

    private void sendToDeadLetter(ConsumerRecord<?, ?> record, Exception exception) {
        try {
            deadLetter.accept(record, exception);
            log.error("contract failure sent to dead letter topic={} partition={} offset={} key={}",
                    record.topic(), record.partition(), record.offset(), record.key(), exception);
        } catch (Exception publishFailure) {
            publishFailure.addSuppressed(exception);
            stopIngestion(record, publishFailure, "the dead letter topic is unreachable");
        }
    }

    private void stopIngestion(ConsumerRecord<?, ?> record, Exception exception, String reason) {
        log.error("{}, stopping ingestion on this instance. topic={} partition={} offset={} key={}",
                reason, record.topic(), record.partition(), record.offset(), record.key(), exception);
        registry.getObject().stop();
    }
}
