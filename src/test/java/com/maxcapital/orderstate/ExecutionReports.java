package com.maxcapital.orderstate;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;
import com.maxcapital.orderstate.model.OrderStatus;

import java.math.BigDecimal;

final class ExecutionReports {

    private ExecutionReports() {
    }

    static ExecutionReportMessage er(String fixId, long numericOrderId, OrderStatus status) {
        return er(fixId, numericOrderId, status, 0, 4956);
    }

    static ExecutionReportMessage er(String fixId, long numericOrderId, OrderStatus status, long accumulative, long leaves) {
        return new ExecutionReportMessage(
                fixId,
                numericOrderId,
                status,
                BigDecimal.valueOf(4956),
                BigDecimal.valueOf(accumulative),
                BigDecimal.valueOf(leaves));
    }

    static String raw(ExecutionReportMessage report) {
        return """
                {"fixId":"%s","numericOrderId":%d,"status":"%s","marketOrderId":"O0S6tDQoQqVy",\
                "ticker":"VSCPC","side":"BUY","securityType":"COMMON_STOCK","orderPrice":104.25,\
                "nominalAmounts":%s,"accumulativeNominalAmount":%s,"leavesNominalAmount":%s,\
                "executionNominalAmount":800,"executionPrice":104.30,"avgPrice":104.28,\
                "secondaryTradeId":"STID-%s","operationNumber":"OP-%s",\
                "transactionTime":"2026-07-20T18:08:52.129"}"""
                .formatted(report.fixId(), report.numericOrderId(), report.status(),
                        report.nominalAmounts(), report.accumulativeNominalAmount(), report.leavesNominalAmount(),
                        report.fixId(), report.fixId());
    }
}
