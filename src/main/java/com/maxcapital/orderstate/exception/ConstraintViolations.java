package com.maxcapital.orderstate.exception;

import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;

public final class ConstraintViolations {

    private ConstraintViolations() {
    }

    public static boolean violates(DataIntegrityViolationException failure, String constraint) {
        return failure.getCause() instanceof ConstraintViolationException cause
                && constraint.equals(cause.getConstraintName());
    }
}
