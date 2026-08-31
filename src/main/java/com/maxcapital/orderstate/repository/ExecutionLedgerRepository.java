package com.maxcapital.orderstate.repository;

import com.maxcapital.orderstate.model.ExecutionLedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ExecutionLedgerRepository extends JpaRepository<ExecutionLedgerEntry, Long> {

    List<ExecutionLedgerEntry> findByNumericOrderIdOrderByIdAsc(Long numericOrderId);
}
