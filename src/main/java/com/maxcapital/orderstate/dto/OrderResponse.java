package com.maxcapital.orderstate.dto;

import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import lombok.Builder;

import java.util.List;

@Builder
public record OrderResponse(
        Long numericOrderId,
        OrderStatus status,
        int appliedExecutions,
        List<LedgerEntryResponse> ledger
) {
    public static OrderResponse from(Order order, List<LedgerEntryResponse> ledger) {
        return OrderResponse.builder()
                .numericOrderId(order.getNumericOrderId())
                .status(order.getStatus())
                .appliedExecutions(order.getAppliedExecutions())
                .ledger(ledger)
                .build();
    }
}
