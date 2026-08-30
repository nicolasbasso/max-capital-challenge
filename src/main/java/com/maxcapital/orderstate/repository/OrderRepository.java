package com.maxcapital.orderstate.repository;

import com.maxcapital.orderstate.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
