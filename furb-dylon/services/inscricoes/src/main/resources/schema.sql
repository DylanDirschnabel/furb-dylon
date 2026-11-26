CREATE TABLE IF NOT EXISTS vagas (
    id        INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    total     INT NOT NULL,
    ocupadas  INT NOT NULL DEFAULT 0 CHECK (ocupadas >= 0 AND ocupadas <= total)
);

CREATE TABLE IF NOT EXISTS inscricoes (
    id               UUID PRIMARY KEY,
    matricula        TEXT NOT NULL,
    email            TEXT NOT NULL,
    status           TEXT NOT NULL,
    motivo           TEXT,
    secret_name      TEXT,
    prazo_validacao  TIMESTAMPTZ,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS inscricao_ativa_por_matricula
    ON inscricoes (matricula) WHERE status <> 'RECUSADA';

CREATE TABLE IF NOT EXISTS mensagens_processadas (
    message_id     UUID PRIMARY KEY,
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
