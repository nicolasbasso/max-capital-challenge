package com.maxcapital.orderstate.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;

import java.math.BigDecimal;

@Embeddable
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ExecutionAmounts {

    @Column(name = "nominal_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal nominalAmount;

    @Column(name = "accumulative_nominal_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal accumulativeNominalAmount;

    @Column(name = "leaves_nominal_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal leavesNominalAmount;

    public static ExecutionAmounts of(BigDecimal nominalAmount, BigDecimal accumulativeNominalAmount, BigDecimal leavesNominalAmount) {
        return ExecutionAmounts.builder()
                .nominalAmount(nominalAmount)
                .accumulativeNominalAmount(accumulativeNominalAmount)
                .leavesNominalAmount(leavesNominalAmount)
                .build();
    }

    public static ExecutionAmounts zero() {
        return of(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
    }
}
