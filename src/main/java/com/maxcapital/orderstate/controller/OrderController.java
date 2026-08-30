package com.maxcapital.orderstate.controller;

import com.maxcapital.orderstate.dto.OrderResponse;
import com.maxcapital.orderstate.service.OrderQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderQueryService orderQueryService;

    @GetMapping("/{numericOrderId}")
    public OrderResponse getByNumericOrderId(@PathVariable Long numericOrderId) {
        return orderQueryService.getByNumericOrderId(numericOrderId);
    }
}
