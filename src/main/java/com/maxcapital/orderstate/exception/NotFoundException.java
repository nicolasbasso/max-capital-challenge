package com.maxcapital.orderstate.exception;

import org.springframework.http.HttpStatus;

public class NotFoundException extends BusinessException {
    protected NotFoundException(String code, String message) {
        super(code, message, HttpStatus.NOT_FOUND);
    }
}
