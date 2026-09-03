package com.maxcapital.orderstate.config;

import jakarta.validation.Valid;
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
@ConfigurationProperties(prefix = "app.settlement")
public class SettlementConfigurations {

    @NotBlank
    private String topic;

    @NotNull
    private Duration publishTimeout;

    @Valid
    @NotNull
    private Sweep sweep;

    @Getter
    @Setter
    public static class Sweep {

        @NotNull
        private Duration interval;

        @Positive
        private int batchSize;
    }
}
