package com.maxcapital.orderstate.handler;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.service.ExecutionReportService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionReportConsumer {

    private final ExecutionReportService executionReportService;

    @KafkaListener(topics = "${app.kafka.execution-reports-topic}")
    public void onExecutionReport(@Valid @Payload ExecutionReportMessage report,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Header(KafkaHeaders.OFFSET) long offset, Acknowledgment acknowledgment) {
        try {
            executionReportService.apply(report);
            log.info("applied numericOrderId={} fixId={} partition={} offset={}", report.numericOrderId(), report.fixId(), partition, offset);
        } catch (DuplicateExecutionReportException alreadyApplied) {
            log.info("duplicate ignored numericOrderId={} fixId={} partition={} offset={}", report.numericOrderId(), report.fixId(), partition, offset);
        }

        acknowledgment.acknowledge();
    }
}
