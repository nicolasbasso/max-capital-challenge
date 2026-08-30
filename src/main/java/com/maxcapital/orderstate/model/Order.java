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

    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private Instant updatedAt;

    public static Order opening(Long numericOrderId, OrderStatus status) {
        return Order.builder()
                .numericOrderId(numericOrderId)
                .status(status)
                .appliedExecutions(0)
                .build();
    }

    public void applyExecution(OrderStatus incoming) {
        this.status = incoming;
        this.appliedExecutions = this.appliedExecutions + 1;
    }
}
