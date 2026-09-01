package com.maxcapital.orderstate.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.model.QuarantineReason;
import com.maxcapital.orderstate.model.QuarantinedExecutionReport;
import lombok.Builder;

import java.math.BigDecimal;

import java.time.Instant;

@Builder
public record QuarantinedEntryResponse(
        Long id,
        String fixId,
        OrderStatus incomingStatus,
        OrderStatus orderStatusAtRejection,
        QuarantineReason reason,
        BigDecimal nominalAmount,
        BigDecimal accumulativeNominalAmount,
        BigDecimal leavesNominalAmount,
        @JsonRawValue @JsonProperty(access = JsonProperty.Access.READ_ONLY) String rawPayload,
        Instant recordedAt
) {
    public static QuarantinedEntryResponse from(QuarantinedExecutionReport entry) {
        return QuarantinedEntryResponse.builder()
                .id(entry.getId())
                .fixId(entry.getFixId())
                .incomingStatus(entry.getIncomingStatus())
                .orderStatusAtRejection(entry.getOrderStatusAtRejection())
                .reason(entry.getReason())
                .nominalAmount(entry.getAmounts().getNominalAmount())
                .accumulativeNominalAmount(entry.getAmounts().getAccumulativeNominalAmount())
                .leavesNominalAmount(entry.getAmounts().getLeavesNominalAmount())
                .rawPayload(entry.getRawPayload())
                .recordedAt(entry.getRecordedAt())
                .build();
    }
}
