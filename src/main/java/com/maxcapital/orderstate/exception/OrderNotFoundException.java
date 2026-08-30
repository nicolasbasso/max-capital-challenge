package com.maxcapital.orderstate.exception;

public class OrderNotFoundException extends NotFoundException {
    public OrderNotFoundException(Long numericOrderId) {
        super("ORDER_NOT_FOUND", "Order not found with numericOrderId: " + numericOrderId);
    }
}
