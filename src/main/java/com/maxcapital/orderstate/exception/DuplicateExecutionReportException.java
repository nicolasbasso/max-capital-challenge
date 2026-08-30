package com.maxcapital.orderstate.exception;

public class DuplicateExecutionReportException extends ConflictException {
    public DuplicateExecutionReportException(Long numericOrderId, String fixId) {
        super("EXECUTION_REPORT_ALREADY_APPLIED", "Execution report already applied for numericOrderId: " + numericOrderId + ", fixId: " + fixId);
    }
}
