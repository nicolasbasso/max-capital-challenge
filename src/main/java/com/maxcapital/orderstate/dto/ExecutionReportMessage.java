package com.maxcapital.orderstate.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.maxcapital.orderstate.model.ExecutionAmounts;
import com.maxcapital.orderstate.model.OrderStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ExecutionReportMessage(

        @NotBlank
        @Size(max = 64)
        String fixId,

        @NotNull
        Long numericOrderId,

        @NotNull
        OrderStatus status,

        @NotNull
        @PositiveOrZero
        BigDecimal nominalAmounts,

        @NotNull
        @PositiveOrZero
        BigDecimal accumulativeNominalAmount,

        @NotNull
        @PositiveOrZero
        BigDecimal leavesNominalAmount
) {
    public ExecutionAmounts amounts() {
        return ExecutionAmounts.of(nominalAmounts, accumulativeNominalAmount, leavesNominalAmount);
    }
}
