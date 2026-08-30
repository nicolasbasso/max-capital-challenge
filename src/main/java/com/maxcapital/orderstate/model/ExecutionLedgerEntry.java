package com.maxcapital.orderstate.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(
        name = "execution_ledger",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_execution_ledger_order_fix",
                columnNames = {"numeric_order_id", "fix_id"}
        )
)
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ExecutionLedgerEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "numeric_order_id", nullable = false)
    private Long numericOrderId;

    @Column(name = "fix_id", nullable = false, length = 64)
    private String fixId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private OrderStatus status;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;

    public static ExecutionLedgerEntry applied(Long numericOrderId, String fixId, OrderStatus status) {
        return ExecutionLedgerEntry.builder()
                .numericOrderId(numericOrderId)
                .fixId(fixId)
                .status(status)
                .recordedAt(Instant.now())
                .build();
    }
}
