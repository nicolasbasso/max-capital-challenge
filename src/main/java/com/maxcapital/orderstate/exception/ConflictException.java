package com.maxcapital.orderstate.exception;

import org.springframework.http.HttpStatus;

public class ConflictException extends BusinessException {
    protected ConflictException(String code, String message) {
        super(code, message, HttpStatus.CONFLICT);
    }
}
