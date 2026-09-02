package com.maxcapital.orderstate.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.maxcapital.orderstate.handler.ExecutionReportFailureRecoverer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

@Configuration
@RequiredArgsConstructor
public class KafkaValidationConfiguration implements KafkaListenerConfigurer {

    private final LocalValidatorFactoryBean validator;
    private final KafkaConfigurations kafkaConfigurations;

    @Bean
    RecordMessageConverter executionReportMessageConverter(ObjectMapper objectMapper) {
        ObjectMapper strict = objectMapper.copy();
        strict.enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS);
        strict.coercionConfigFor(LogicalType.Integer)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        return new StringJsonMessageConverter(strict);
    }

    @Bean
    DeadLetterPublishingRecoverer executionReportDeadLetterRecoverer(KafkaOperations<String, String> kafkaOperations) {
        return new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> new TopicPartition(
                        kafkaConfigurations.getDeadLetterTopic(), record.partition()));
    }

    @Bean(destroyMethod = "destroy")
    ThreadPoolTaskScheduler executionReportBackOffScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("er-backoff-");
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    ListenerContainerPauseService executionReportPauseService(KafkaListenerEndpointRegistry registry,
                                                              TaskScheduler executionReportBackOffScheduler) {
        return new ListenerContainerPauseService(registry, executionReportBackOffScheduler);
    }

    @Bean
    DefaultErrorHandler executionReportErrorHandler(ExecutionReportFailureRecoverer failureRecoverer,
                                                   ListenerContainerPauseService pauseService,
                                                   ExponentialBackOff transientBackOff) {

        DefaultErrorHandler handler = new DefaultErrorHandler(
                failureRecoverer,
                transientBackOff,
                new ContainerPausingBackOffHandler(pauseService));

        handler.defaultFalse();
        ExecutionReportFailureRecoverer.transientFailures().forEach(handler::addRetryableExceptions);

        return handler;
    }

    @Bean
    ExponentialBackOff transientBackOff() {//TODO: son properties desde applicationProperties, propongo hacer distintos objetos
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2);
        backOff.setMaxInterval(10_000);
        backOff.setMaxAttempts(kafkaConfigurations.getRetryMaxAttempts());
        return backOff;
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setValidator(validator);
    }
}
