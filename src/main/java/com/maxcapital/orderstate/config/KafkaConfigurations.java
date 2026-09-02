package com.maxcapital.orderstate.config;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaConfigurations {

    @NotBlank
    private String executionReportsTopic;

    @NotBlank
    private String deadLetterTopic;

    @NotNull
    private Duration retryInitialInterval;

    @DecimalMin("1.0")
    private double retryMultiplier;

    @NotNull
    private Duration retryMaxInterval;

    @Positive
    private int retryMaxAttempts;

    @Positive
    private int partitions;

    @Positive
    private int replicas;
}
