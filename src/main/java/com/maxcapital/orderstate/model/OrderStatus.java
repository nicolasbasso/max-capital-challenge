package com.maxcapital.orderstate.model;

public enum OrderStatus {
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    INCOMPLETE;

    public static boolean applies(OrderStatus persisted, OrderStatus incoming) {
        if (incoming == INCOMPLETE) {
            return false;
        }
        if (persisted == null) {
            return incoming == NEW;
        }
        return incoming != NEW && (persisted == NEW || persisted == PARTIALLY_FILLED);
    }
}
