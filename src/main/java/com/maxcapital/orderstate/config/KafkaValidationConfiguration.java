package com.maxcapital.orderstate.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaValidationConfiguration implements KafkaListenerConfigurer {

    private final LocalValidatorFactoryBean validator;

    @Bean
    RecordMessageConverter executionReportMessageConverter() {
        return new StringJsonMessageConverter();
    }

    @Bean
    DefaultErrorHandler executionReportErrorHandler() {
        return new DefaultErrorHandler((record, exception) -> log.error(
                "execution report failed topic={} partition={} offset={} key={} payload={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), exception));
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setValidator(validator);
    }
}
