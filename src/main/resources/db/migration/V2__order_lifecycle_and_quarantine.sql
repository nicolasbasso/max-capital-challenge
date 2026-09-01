-- Slice 2: la orden atraviesa su ciclo de vida y los ER rechazados dejan de perderse.

-- Los campos de cantidad son snapshots acumulados, no deltas: la orden copia el ultimo valor
-- recibido, nunca suma. Por eso viven como columnas de la orden y tambien de cada entrada del
-- ledger, que es la foto de lo que traia ese ER.
ALTER TABLE orders
    ADD COLUMN nominal_amount              NUMERIC(19, 4) NOT NULL DEFAULT 0,
    ADD COLUMN accumulative_nominal_amount NUMERIC(19, 4) NOT NULL DEFAULT 0,
    ADD COLUMN leaves_nominal_amount       NUMERIC(19, 4) NOT NULL DEFAULT 0;

-- Que campo es columna y que queda en el crudo sigue un solo criterio: tiene columna lo que el
-- sistema LEE PARA DECIDIR algo. Los tres montos estan porque alimentan la validacion de coherencia
-- y el estado de la orden; el total tambien, para que la fila se pueda reverificar sola sin ir a
-- buscarlo a otra tabla.
--
-- Todo lo demas vive en raw_payload y no se pierde nada: marketOrderId, ticker, side, securityType,
-- orderPrice, executionNominalAmount, executionPrice, avgPrice, secondaryTradeId, operationNumber y
-- transactionTime. El crudo ademas preserva lo que NO modelamos: el DTO declara ignoreUnknown y el
-- mensaje del enunciado termina en "...", asi que columnas tipadas solo podrian guardar la lista de
-- hoy. Y preserva el mensaje literal, que es lo unico que permite reprocesarlo tal cual llego.
--
-- Las columnas no son un almacen paralelo: son un indice sobre el crudo. Se agrega una cuando hace
-- falta filtrar por ese campo, no para poder reconstruirlo, porque eso ya esta garantizado.
--
-- El crudo va en las dos tablas: el ledger es el registro de auditoria del ciclo de vida de la orden,
-- y un registro de auditoria que guarda menos de lo que llego no sirve para reconstruir nada.
--
-- transactionTime queda deliberadamente sin columna. Tenerla invitaria a ordenar por ella, y es reloj
-- de pared del origen: no es criterio de secuencia. El orden lo da id (D-001).
ALTER TABLE execution_ledger
    ADD COLUMN nominal_amount              NUMERIC(19, 4) NOT NULL DEFAULT 0,
    ADD COLUMN accumulative_nominal_amount NUMERIC(19, 4) NOT NULL DEFAULT 0,
    ADD COLUMN leaves_nominal_amount       NUMERIC(19, 4) NOT NULL DEFAULT 0,
    ADD COLUMN raw_payload                 JSONB          NOT NULL;

-- D-005: donde vive el ER preservado. Un ER integro y atribuible que la maquina de estados
-- rechaza no entra al ledger, porque el ledger es "una entrada por ER efectivamente aplicado"
-- y el contador tiene que seguir igualando la cantidad de filas.
--
-- La constraint unica no es decorativa: es lo que convierte la proteccion de la reentrega de un
-- ER rechazado en proteccion por identidad. Sin ella, la reentrega dependeria unicamente de que
-- INCOMPLETE sea absorbente.
--
-- reason tiene un solo valor desde el slice 2:
--   STATE_TRANSITION_REJECTED  el ER no encaja en la maquina de estados de D-005
-- No se valida coherencia interna de montos: el enunciado no la exige, y bajo la regla de D-005
-- cualquier validacion extra es una forma mas de congelar ordenes. Un CANCELLED que reporta
-- remanente cero despues de un fill parcial es legitimo y quedaba congelado.
-- D-006 decide si escribe motivos propios en esta tabla.
--
-- Los montos se repiten como columnas aca por el mismo criterio que en el ledger: una fila de
-- cuarentena que no muestra los montos no contesta la pregunta que la tabla existe para contestar.
CREATE TABLE execution_quarantine (
    id                        BIGSERIAL    PRIMARY KEY,
    numeric_order_id          BIGINT       NOT NULL REFERENCES orders (numeric_order_id),
    fix_id                    VARCHAR(64)  NOT NULL,
    incoming_status           VARCHAR(32)  NOT NULL,
    order_status_at_rejection VARCHAR(32),
    reason                    VARCHAR(64)  NOT NULL,

    nominal_amount              NUMERIC(19, 4) NOT NULL,
    accumulative_nominal_amount NUMERIC(19, 4) NOT NULL,
    leaves_nominal_amount       NUMERIC(19, 4) NOT NULL,

    raw_payload               JSONB        NOT NULL,
    recorded_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_execution_quarantine_order_fix UNIQUE (numeric_order_id, fix_id)
);

-- reason tiene un solo valor desde el slice 2:
--   STATE_TRANSITION_REJECTED  el ER no encaja en la maquina de estados de D-005

-- order_status_at_rejection es nullable a proposito: cuando el ER llega y la orden no existe
-- todavia, no hay estado previo que registrar. Es la fila "(no existe)" de la tabla de D-005.

CREATE INDEX idx_execution_quarantine_order ON execution_quarantine (numeric_order_id, id);
