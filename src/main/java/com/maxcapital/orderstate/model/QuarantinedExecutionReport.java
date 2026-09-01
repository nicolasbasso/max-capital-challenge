package com.maxcapital.orderstate.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.generator.EventType;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(
        name = "execution_quarantine",
        uniqueConstraints = @UniqueConstraint(
                name = QuarantinedExecutionReport.UNIQUE_ORDER_FIX_ID,
                columnNames = {"numeric_order_id", "fix_id"}
        )
)
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class QuarantinedExecutionReport {

    public static final String UNIQUE_ORDER_FIX_ID = "uq_execution_quarantine_order_fix";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "numeric_order_id", nullable = false)
    private Long numericOrderId;

    @Column(name = "fix_id", nullable = false, length = 64)
    private String fixId;

    @Enumerated(EnumType.STRING)
    @Column(name = "incoming_status", nullable = false, length = 32)
    private OrderStatus incomingStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "order_status_at_rejection", length = 32)
    private OrderStatus orderStatusAtRejection;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 64)
    private QuarantineReason reason;

    @Embedded
    private ExecutionAmounts amounts;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw_payload", nullable = false, updatable = false)
    private String rawPayload;

    @Generated(event = EventType.INSERT)
    @Column(name = "recorded_at", nullable = false, insertable = false, updatable = false)
    private Instant recordedAt;

    public static QuarantinedExecutionReport rejected(Long numericOrderId, String fixId, OrderStatus incomingStatus,
                                                      OrderStatus orderStatusAtRejection, QuarantineReason reason,
                                                      ExecutionAmounts amounts, String rawPayload) {
        return QuarantinedExecutionReport.builder()
                .numericOrderId(numericOrderId)
                .fixId(fixId)
                .incomingStatus(incomingStatus)
                .orderStatusAtRejection(orderStatusAtRejection)
                .reason(reason)
                .amounts(amounts)
                .rawPayload(rawPayload)
                .build();
    }
}
