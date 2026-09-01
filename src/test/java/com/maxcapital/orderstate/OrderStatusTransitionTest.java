package com.maxcapital.orderstate;

import com.maxcapital.orderstate.model.OrderStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static com.maxcapital.orderstate.model.OrderStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class OrderStatusTransitionTest {

    @ParameterizedTest(name = "desde {0} llega {1} -> aplica={2}")
    @CsvSource(nullValues = "AUSENTE", value = {
            "AUSENTE,          NEW,              true",
            "AUSENTE,          PARTIALLY_FILLED, false",
            "AUSENTE,          FILLED,           false",
            "AUSENTE,          CANCELLED,        false",

            "NEW,              NEW,              false",
            "NEW,              PARTIALLY_FILLED, true",
            "NEW,              FILLED,           true",
            "NEW,              CANCELLED,        true",

            "PARTIALLY_FILLED, NEW,              false",
            "PARTIALLY_FILLED, PARTIALLY_FILLED, true",
            "PARTIALLY_FILLED, FILLED,           true",
            "PARTIALLY_FILLED, CANCELLED,        true",

            "FILLED,           NEW,              false",
            "FILLED,           PARTIALLY_FILLED, false",
            "FILLED,           FILLED,           false",
            "FILLED,           CANCELLED,        false",

            "CANCELLED,        NEW,              false",
            "CANCELLED,        PARTIALLY_FILLED, false",
            "CANCELLED,        FILLED,           false",
            "CANCELLED,        CANCELLED,        false",

            "INCOMPLETE,       NEW,              false",
            "INCOMPLETE,       PARTIALLY_FILLED, false",
            "INCOMPLETE,       FILLED,           false",
            "INCOMPLETE,       CANCELLED,        false"
    })
    void laTablaDeD005EnteraCeldaPorCelda(OrderStatus persisted, OrderStatus incoming, boolean aplica) {
        assertThat(OrderStatus.applies(persisted, incoming)).isEqualTo(aplica);
    }

    @ParameterizedTest
    @CsvSource(nullValues = "AUSENTE", value = {"AUSENTE", "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "INCOMPLETE"})
    void unErQueDeclaraIncompleteNoSeAplicaNunca(OrderStatus persisted) {
        assertThat(OrderStatus.applies(persisted, INCOMPLETE))
                .as("INCOMPLETE es un valor de nuestro dominio, ningun ER del mercado puede traerlo")
                .isFalse();
    }
}
