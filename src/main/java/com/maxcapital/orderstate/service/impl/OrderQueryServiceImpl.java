package com.maxcapital.orderstate.service.impl;

import com.maxcapital.orderstate.dto.LedgerEntryResponse;
import com.maxcapital.orderstate.dto.OrderResponse;
import com.maxcapital.orderstate.dto.QuarantinedEntryResponse;
import com.maxcapital.orderstate.exception.OrderNotFoundException;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.repository.QuarantineRepository;
import com.maxcapital.orderstate.service.OrderQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderQueryServiceImpl implements OrderQueryService {
    private final OrderRepository orderRepository;
    private final ExecutionLedgerRepository executionLedgerRepository;
    private final QuarantineRepository quarantineRepository;

    @Override
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public OrderResponse getByNumericOrderId(Long numericOrderId) {
        Order order = orderRepository.findById(numericOrderId)
                .orElseThrow(() -> new OrderNotFoundException(numericOrderId));

        List<LedgerEntryResponse> ledger = executionLedgerRepository
                .findByNumericOrderIdOrderByIdAsc(numericOrderId)
                .stream()
                .map(LedgerEntryResponse::from)
                .toList();

        List<QuarantinedEntryResponse> quarantine = quarantineRepository
                .findByNumericOrderIdOrderByIdAsc(numericOrderId)
                .stream()
                .map(QuarantinedEntryResponse::from)
                .toList();

        return OrderResponse.from(order, ledger, quarantine);
    }
}
