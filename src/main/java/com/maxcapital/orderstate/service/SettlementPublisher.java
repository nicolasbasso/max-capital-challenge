package com.maxcapital.orderstate.service;

public interface SettlementPublisher {

    void publishPendingSettlements();

    void publishPendingIncompleteNotices();
}
