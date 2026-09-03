package com.maxcapital.orderstate.dto;

import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Builder
public record OrderResponse(
        Long numericOrderId,
        OrderStatus status,
        int appliedExecutions,
        BigDecimal nominalAmount,
        BigDecimal accumulativeNominalAmount,
        BigDecimal leavesNominalAmount,
        Instant settlementPublishedAt,
        Instant markedIncompleteNotifiedAt,
        List<LedgerEntryResponse> ledger,
        List<QuarantinedEntryResponse> quarantine
) {
    public static OrderResponse from(Order order, List<LedgerEntryResponse> ledger, List<QuarantinedEntryResponse> quarantine) {
        return OrderResponse.builder()
                .numericOrderId(order.getNumericOrderId())
                .status(order.getStatus())
                .appliedExecutions(order.getAppliedExecutions())
                .nominalAmount(order.getAmounts().getNominalAmount())
                .accumulativeNominalAmount(order.getAmounts().getAccumulativeNominalAmount())
                .leavesNominalAmount(order.getAmounts().getLeavesNominalAmount())
                .settlementPublishedAt(order.getSettlementPublishedAt())
                .markedIncompleteNotifiedAt(order.getMarkedIncompleteNotifiedAt())
                .ledger(ledger)
                .quarantine(quarantine)
                .build();
    }
}
