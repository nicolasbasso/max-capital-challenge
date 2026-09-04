package com.maxcapital.orderstate.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.orderstate.config.SettlementConfigurations;
import com.maxcapital.orderstate.config.SettlementSchedulerConfiguration;
import com.maxcapital.orderstate.dto.OrderEventMessage;
import com.maxcapital.orderstate.model.Order;
import com.maxcapital.orderstate.repository.OrderRepository;
import com.maxcapital.orderstate.service.SettlementPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

@Slf4j
@Service
@RequiredArgsConstructor
public class SettlementPublisherImpl implements SettlementPublisher {

    private static final Pageable ONE = Pageable.ofSize(1);

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SettlementConfigurations settlementConfigurations;
    private final TransactionTemplate transactionTemplate;

    @Override
    @Scheduled(fixedDelayString = "${app.settlement.sweep.interval}",
            scheduler = SettlementSchedulerConfiguration.SETTLEMENT_SCHEDULER)
    public int publishPendingSettlements() {
        return sweep(this::publishNextSettlement);
    }

    @Override
    @Scheduled(fixedDelayString = "${app.settlement.sweep.interval}",
            scheduler = SettlementSchedulerConfiguration.SETTLEMENT_SCHEDULER)
    public int publishPendingIncompleteNotices() {
        return sweep(this::publishNextIncompleteNotice);
    }

    private int sweep(BooleanSupplier publishNext) {
        int published = 0;
        while (published < settlementConfigurations.getSweep().getBatchSize() && publishNext.getAsBoolean()) {
            published++;
        }
        return published;
    }

    private boolean publishNextSettlement() {
        return Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            List<Order> pending = orderRepository.lockOrdersPendingSettlement(ONE);
            if (pending.isEmpty()) {
                return false;
            }
            Order order = pending.getFirst();
            publish(OrderEventMessage.settled(order.getNumericOrderId()));
            order.settlementPublished(Instant.now());
            orderRepository.save(order);
            log.info("settlement published numericOrderId={}", order.getNumericOrderId());
            return true;
        }));
    }

    private boolean publishNextIncompleteNotice() {
        return Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            List<Order> pending = orderRepository.lockOrdersPendingIncompleteNotice(ONE);
            if (pending.isEmpty()) {
                return false;
            }
            Order order = pending.getFirst();
            publish(OrderEventMessage.markedIncomplete(order.getNumericOrderId()));
            order.markedIncompleteNotified(Instant.now());
            orderRepository.save(order);
            log.info("incomplete notice published numericOrderId={}", order.getNumericOrderId());
            return true;
        }));
    }

    private void publish(OrderEventMessage event) {
        try {
            kafkaTemplate.send(settlementConfigurations.getTopic(),
                            String.valueOf(event.numericOrderId()),
                            objectMapper.writeValueAsString(event))
                    .get(settlementConfigurations.getPublishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(notPublished(event), interrupted);
        } catch (JsonProcessingException | ExecutionException | TimeoutException | KafkaException failure) {
            throw new IllegalStateException(notPublished(event), failure);
        }
    }

    private static String notPublished(OrderEventMessage event) {
        return "could not publish %s for numericOrderId=%d, the order stays unmarked and the next sweep retries"
                .formatted(event.type(), event.numericOrderId());
    }
}
