CREATE TABLE IF NOT EXISTS notas (
    matricula      TEXT NOT NULL,
    disciplina     TEXT NOT NULL,
    avaliacao      TEXT NOT NULL,
    valor          NUMERIC(4,2) NOT NULL,
    coletado_em    TIMESTAMPTZ NOT NULL,
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (matricula, disciplina, avaliacao)
);

CREATE TABLE IF NOT EXISTS mensagens_processadas (
    message_id     UUID PRIMARY KEY,
    processado_por TEXT,
    processado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    destino_tipo  TEXT NOT NULL,
    destino       TEXT NOT NULL,
    corpo         TEXT NOT NULL,
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    publicado_em  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_pendentes ON outbox (id) WHERE publicado_em IS NULL;

ALTER TABLE mensagens_processadas ADD COLUMN IF NOT EXISTS processado_por TEXT;

CREATE TABLE IF NOT EXISTS coletas (
    matricula      TEXT PRIMARY KEY,
    coletado_em    TIMESTAMPTZ NOT NULL,
    processado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);
