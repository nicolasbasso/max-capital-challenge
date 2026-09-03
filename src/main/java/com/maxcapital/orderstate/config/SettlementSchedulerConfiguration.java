package com.maxcapital.orderstate.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@RequiredArgsConstructor
public class SettlementSchedulerConfiguration {

    public static final String SETTLEMENT_SCHEDULER = "settlementScheduler";

    private final SettlementConfigurations settlementConfigurations;

    @Bean(name = SETTLEMENT_SCHEDULER, destroyMethod = "destroy")
    ThreadPoolTaskScheduler settlementScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(settlementConfigurations.getSweep().getPoolSize());
        scheduler.setThreadNamePrefix("settlement-");
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        scheduler.initialize();
        return scheduler;
    }
}
