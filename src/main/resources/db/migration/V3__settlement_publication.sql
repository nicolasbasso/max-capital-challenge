-- Slice 7: el settlement se publica desde un barrido, no desde la transaccion que aplica el ER.

-- Dos marcas de lo que ya le dijimos a downstream. Son nullable a proposito: null significa
-- "todavia no se lo dijimos", y esa es toda la maquinaria del outbox. No hace falta una tabla
-- aparte porque el payload es solo el id, y porque los campos estables de la orden ya viven aca.
--
-- Ademas quedan visibles en el GET: la conversacion con downstream se consulta, no se busca en logs.
ALTER TABLE orders
    ADD COLUMN settlement_published_at  TIMESTAMPTZ,
    ADD COLUMN marked_incomplete_notified_at TIMESTAMPTZ;

-- Indices parciales: contienen SOLO las filas pendientes, asi que el barrido vacio -que es casi
-- siempre- no escanea orders. Una orden que ya se publico sale del indice y no vuelve.
CREATE INDEX idx_orders_pending_settlement
    ON orders (numeric_order_id)
    WHERE status = 'FILLED' AND settlement_published_at IS NULL;

-- La orden que settleo y despues quedo incompleta. Se apoya en el estado actual y es correcto
-- hacerlo: de INCOMPLETE no se sale, a diferencia de FILLED.
CREATE INDEX idx_orders_pending_incomplete_notice
    ON orders (numeric_order_id)
    WHERE status = 'INCOMPLETE' AND settlement_published_at IS NOT NULL
      AND marked_incomplete_notified_at IS NULL;
