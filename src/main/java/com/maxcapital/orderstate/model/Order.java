package com.maxcapital.orderstate.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Generated;
import org.hibernate.generator.EventType;

import java.time.Instant;

@Entity
@DynamicUpdate
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

    @Column(name = "settlement_published_at")
    private Instant settlementPublishedAt;

    @Column(name = "marked_incomplete_notified_at")
    private Instant markedIncompleteNotifiedAt;

    public static Order opening(Long numericOrderId) {
        return Order.builder()
                .numericOrderId(numericOrderId)
                .status(OrderStatus.NEW)
                .appliedExecutions(0) //TODO: hace falta? int no inicializa en 0 ?
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

    public void settlementPublished(Instant at) { //TODO: usar lombok
        this.settlementPublishedAt = at;
    }

    public void markedIncompleteNotified(Instant at) { //TODO: usar lombok
        this.markedIncompleteNotifiedAt = at;
    }
}
