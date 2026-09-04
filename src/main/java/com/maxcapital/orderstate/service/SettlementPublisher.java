package com.maxcapital.orderstate.service;

public interface SettlementPublisher {

    int publishPendingSettlements();

    int publishPendingIncompleteNotices();
}
