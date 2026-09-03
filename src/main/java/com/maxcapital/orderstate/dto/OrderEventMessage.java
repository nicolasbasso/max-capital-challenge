package com.maxcapital.orderstate.dto;

public record OrderEventMessage(OrderEventType type, Long numericOrderId) {

    public static OrderEventMessage settled(Long numericOrderId) {
        return new OrderEventMessage(OrderEventType.ORDER_SETTLED, numericOrderId);
    }

    public static OrderEventMessage markedIncomplete(Long numericOrderId) {
        return new OrderEventMessage(OrderEventType.ORDER_MARKED_INCOMPLETE, numericOrderId);
    }
}
