package com.maxcapital.orderstate.dto;

import com.maxcapital.orderstate.model.ExecutionLedgerEntry;
import com.maxcapital.orderstate.model.OrderStatus;
import lombok.Builder;

import java.time.Instant;

@Builder
public record LedgerEntryResponse(
        Long id,
        String fixId,
        OrderStatus status,
        Instant recordedAt
) {
    public static LedgerEntryResponse from(ExecutionLedgerEntry entry) {
        return LedgerEntryResponse.builder()
                .id(entry.getId())
                .fixId(entry.getFixId())
                .status(entry.getStatus())
                .recordedAt(entry.getRecordedAt())
                .build();
    }
}
