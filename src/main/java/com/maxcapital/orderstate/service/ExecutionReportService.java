package com.maxcapital.orderstate.service;

import com.maxcapital.orderstate.dto.ExecutionReportMessage;

public interface ExecutionReportService {
    void apply(ExecutionReportMessage report);
}
