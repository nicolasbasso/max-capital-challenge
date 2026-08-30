package com.maxcapital.orderstate.service;

import com.maxcapital.orderstate.dto.OrderResponse;

public interface OrderQueryService {
    OrderResponse getByNumericOrderId(Long numericOrderId);
}
