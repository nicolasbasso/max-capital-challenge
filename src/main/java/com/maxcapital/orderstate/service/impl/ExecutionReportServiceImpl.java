package com.maxcapital.orderstate.service.impl;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.exception.ConstraintViolations;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.model.ExecutionLedgerEntry;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.model.OrderStatus;
import com.maxcapital.orderstate.model.QuarantineReason;
import com.maxcapital.orderstate.model.QuarantinedExecutionReport;
import com.maxcapital.orderstate.repository.ExecutionLedgerRepository;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.repository.QuarantineRepository;
import com.maxcapital.orderstate.service.ExecutionReportService;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ExecutionReportServiceImpl implements ExecutionReportService {

    private final OrderRepository orderRepository;
    private final ExecutionLedgerRepository executionLedgerRepository;
    private final QuarantineRepository quarantineRepository;

    @Override
    @Transactional
    public void apply(ExecutionReportMessage report, String rawPayload) {
        Optional<Order> existing = orderRepository.findById(report.numericOrderId());
        OrderStatus statusPersisted = existing.map(Order::getStatus).orElse(null);

        Order order = existing.orElseGet(
                () -> orderRepository.saveAndFlush(Order.opening(report.numericOrderId())));

        ExecutionLedgerEntry provisional = recordInLedger(report, rawPayload);

        Optional<Rejection> rejection = rejectionFor(report, rawPayload, statusPersisted);

        if (rejection.isPresent()) {
            quarantine(order, provisional, rejection.get());
        } else {
            order.applyExecution(report.status(), report.amounts());
        }

        orderRepository.save(order);
    }

    private Optional<Rejection> rejectionFor(ExecutionReportMessage report, String rawPayload, OrderStatus statusPersisted) {
        if (OrderStatus.applies(statusPersisted, report.status())) {
            return Optional.empty();
        }
        return Optional.of(new Rejection(report, rawPayload, statusPersisted, QuarantineReason.STATE_TRANSITION_REJECTED));
    }

    private ExecutionLedgerEntry recordInLedger(ExecutionReportMessage report, String rawPayload) {
        try {
            return executionLedgerRepository.saveAndFlush(ExecutionLedgerEntry.applied(
                    report.numericOrderId(), report.fixId(), report.status(), report.amounts(), rawPayload));
        } catch (DataIntegrityViolationException violation) {
            throw asDuplicateOrRethrow(violation, ExecutionLedgerEntry.UNIQUE_ORDER_FIX_ID, report);
        }
    }

    private void quarantine(Order order, ExecutionLedgerEntry provisional, Rejection rejection) {
        executionLedgerRepository.delete(provisional);
        executionLedgerRepository.flush();

        ExecutionReportMessage report = rejection.report();
        try {
            quarantineRepository.saveAndFlush(QuarantinedExecutionReport.rejected(
                    report.numericOrderId(), report.fixId(), report.status(), rejection.statusPersisted(),
                    rejection.reason(), report.amounts(), rejection.rawPayload()));
        } catch (DataIntegrityViolationException violation) {
            throw asDuplicateOrRethrow(violation, QuarantinedExecutionReport.UNIQUE_ORDER_FIX_ID, report);
        }

        order.freeze();
    }

    private record Rejection(ExecutionReportMessage report, String rawPayload,
                             OrderStatus statusPersisted, QuarantineReason reason) {
    }

    private RuntimeException asDuplicateOrRethrow(DataIntegrityViolationException violation, String constraint, ExecutionReportMessage report) {
        if (ConstraintViolations.violates(violation, constraint)) {
            return new DuplicateExecutionReportException(report.numericOrderId(), report.fixId());
        }
        return violation;
    }
}
