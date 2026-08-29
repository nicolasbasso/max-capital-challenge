package com.maxcapital.orderstate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 0: el contexto de Spring levanta y el servicio arranca.
 *
 * No prueba ninguna garantia del challenge todavia; su unico proposito es que el
 * andamiaje sea verificable antes de que exista logica encima. Si este test se rompe,
 * el problema es del entorno o del wiring, nunca del dominio.
 */
@SpringBootTest
class OrderStateServiceApplicationTests {

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
