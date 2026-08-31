package com.maxcapital.orderstate.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.maxcapital.orderstate.model.OrderStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ExecutionReportMessage(

        @NotBlank
        @Size(max = 64)
        String fixId,

        @NotNull
        Long numericOrderId,

        @NotNull
        OrderStatus status
) {
}
