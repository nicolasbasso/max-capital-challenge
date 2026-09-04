package com.maxcapital.orderstate;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

final class Ordenes {

    private static final AtomicLong SIGUIENTE = new AtomicLong(
            100_000_000L + ThreadLocalRandom.current().nextLong(0, 800_000_000L));

    private Ordenes() {
    }

    static long nueva() {
        return SIGUIENTE.getAndIncrement();
    }
}
