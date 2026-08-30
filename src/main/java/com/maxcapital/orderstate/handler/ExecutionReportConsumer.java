package com.maxcapital.orderstate.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.exception.DuplicateExecutionReportException;
import com.maxcapital.orderstate.service.ExecutionReportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionReportConsumer {
    private final ExecutionReportService executionReportService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.execution-reports-topic}")
    public void onExecutionReport(String payload,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Header(KafkaHeaders.OFFSET) long offset,
                                  Acknowledgment acknowledgment) throws Exception {
        ExecutionReportMessage report = objectMapper.readValue(payload, ExecutionReportMessage.class);

        try {
            executionReportService.apply(report);
            log.info("applied numericOrderId={} fixId={} partition={} offset={}",
                    report.numericOrderId(), report.fixId(), partition, offset);
        } catch (DuplicateExecutionReportException alreadyApplied) {
            log.info("duplicate ignored numericOrderId={} fixId={} partition={} offset={}",
                    report.numericOrderId(), report.fixId(), partition, offset);
        }

        acknowledgment.acknowledge();
    }
}
