package com.maxcapital.orderstate.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Generated;
import org.hibernate.generator.EventType;

import java.time.Instant;

@Entity
@Table(name = "orders")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Order {

    @Id
    @Column(name = "numeric_order_id")
    private Long numericOrderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private OrderStatus status;

    @Column(name = "applied_executions", nullable = false)
    private int appliedExecutions;

    @Embedded
    private ExecutionAmounts amounts;

    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private Instant updatedAt;

    public static Order opening(Long numericOrderId) {
        return Order.builder()
                .numericOrderId(numericOrderId)
                .status(OrderStatus.NEW)
                .appliedExecutions(0)
                .amounts(ExecutionAmounts.zero())
                .build();
    }

    public void applyExecution(OrderStatus incoming, ExecutionAmounts amounts) {
        this.status = incoming;
        this.appliedExecutions = this.appliedExecutions + 1;
        this.amounts = amounts;
    }

    public void freeze() {
        this.status = OrderStatus.INCOMPLETE;
    }
}
