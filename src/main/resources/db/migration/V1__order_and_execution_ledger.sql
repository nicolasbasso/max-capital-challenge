-- Slice 1: lo minimo para que un ER recorra el sistema y se pueda consultar.
-- Los campos cuantitativos entran en el slice 2, cuando participan de una garantia.

CREATE TABLE orders (
    numeric_order_id   BIGINT       PRIMARY KEY,
    status             VARCHAR(32)  NOT NULL,
    applied_executions INTEGER      NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Una entrada por ER efectivamente aplicado.
-- id es BIGSERIAL a proposito: refleja el orden de insercion, que es el orden en que
-- el procesador aplico los ER. No se ordena por transactionTime, que es reloj de pared
-- del origen y no es criterio confiable de secuencia.
CREATE TABLE execution_ledger (
    id               BIGSERIAL    PRIMARY KEY,
    numeric_order_id BIGINT       NOT NULL REFERENCES orders (numeric_order_id),
    fix_id           VARCHAR(64)  NOT NULL,
    status           VARCHAR(32)  NOT NULL,
    recorded_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- D-002: la barrera de idempotencia. Es la unica pieza que ve a las dos instancias
    -- a la vez, porque es la unica que vive donde las dos escriben.
    CONSTRAINT uq_execution_ledger_order_fix UNIQUE (numeric_order_id, fix_id)
);

CREATE INDEX idx_execution_ledger_order ON execution_ledger (numeric_order_id, id);

-- Todos los timestamps los escribe la base. Con dos instancias corriendo, el reloj de cada JVM
-- puede estar corrido; el de la base es el unico que las dos comparten.
-- created_at y recorded_at se escriben una sola vez y les alcanza el DEFAULT.
-- updated_at cambia en cada update, asi que necesita el trigger.

CREATE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
