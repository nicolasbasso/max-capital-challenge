package com.maxcapital.orderstate.config;

import jakarta.validation.Valid;
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

    @Valid
    @NotNull
    private Topics topics;

    @Valid
    @NotNull
    private Retry retry;

    @Positive
    private int partitions;

    @Positive
    private int replicas;

    @Getter
    @Setter
    public static class Topics {

        @NotBlank
        private String executionReports;

        @NotBlank
        private String deadLetter;
    }

    @Getter
    @Setter
    public static class Retry {

        @NotNull
        private Duration initialInterval;

        @DecimalMin("1.0")
        private double multiplier;

        @NotNull
        private Duration maxInterval;

        @Positive
        private int maxAttempts;
    }
}
