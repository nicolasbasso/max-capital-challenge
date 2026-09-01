package com.maxcapital.orderstate.repository;

import com.maxcapital.orderstate.model.QuarantinedExecutionReport;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface QuarantineRepository extends JpaRepository<QuarantinedExecutionReport, Long> {

    List<QuarantinedExecutionReport> findByNumericOrderIdOrderByIdAsc(Long numericOrderId);
}
