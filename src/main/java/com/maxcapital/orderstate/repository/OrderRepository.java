package com.maxcapital.orderstate.repository;

import com.maxcapital.orderstate.model.Order;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, Long> {

    String SKIP_LOCKED = "-2";

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("""
            select o from Order o
            where o.status = com.maxcapital.orderstate.model.OrderStatus.FILLED
              and o.settlementPublishedAt is null
            order by o.numericOrderId
            """)
    List<Order> lockOrdersPendingSettlement(Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("""
            select o from Order o
            where o.status = com.maxcapital.orderstate.model.OrderStatus.INCOMPLETE
              and o.settlementPublishedAt is not null
              and o.markedIncompleteNotifiedAt is null
            order by o.numericOrderId
            """)
    List<Order> lockOrdersPendingIncompleteNotice(Pageable pageable);
}
