package com.maxcapital.orderstate.service.impl;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.model.ExecutionLedgerEntry;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.service.ExecutionReportService;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ExecutionReportServiceImpl implements ExecutionReportService {
    private final OrderRepository orderRepository;
    private final ExecutionLedgerRepository executionLedgerRepository;

    @Override
    @Transactional
    public void apply(ExecutionReportMessage report) {
        Order order = orderRepository.findById(report.numericOrderId())
                .orElseGet(() -> orderRepository.saveAndFlush(
                        Order.opening(report.numericOrderId(), report.status())));

        try {
            executionLedgerRepository.saveAndFlush(ExecutionLedgerEntry.applied(
                    report.numericOrderId(), report.fixId(), report.status()));
        } catch (DataIntegrityViolationException alreadyApplied) {
            throw new DuplicateExecutionReportException(report.numericOrderId(), report.fixId());
        }

        order.applyExecution(report.status());
        orderRepository.save(order);
    }
}
