package com.maxcapital.orderstate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStateServiceApplicationTests extends IntegrationTestBase {
    @Value("${spring.application.name}")
    private String applicationName;

    @Test
    void elContextoLevantaYLaAplicacionEstaIdentificada() {
        assertThat(applicationName).isEqualTo("order-state-service");
    }

    @Test
    void corriendoSobreJava21() {
        assertThat(Runtime.version().feature())
                .as("el enunciado restringe el runtime a Java 21 o 25")
                .isEqualTo(21);
    }
}
