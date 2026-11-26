CREATE USER inscricoes WITH PASSWORD 'inscricoes';
CREATE DATABASE inscricoes OWNER inscricoes;

CREATE USER coletor WITH PASSWORD 'coletor';
CREATE DATABASE coletor OWNER coletor;

CREATE USER notas WITH PASSWORD 'notas';
CREATE DATABASE notas OWNER notas;

CREATE USER notificacao WITH PASSWORD 'notificacao';
CREATE DATABASE notificacao OWNER notificacao;

REVOKE CONNECT ON DATABASE inscricoes, coletor, notas, notificacao FROM PUBLIC;
GRANT CONNECT ON DATABASE inscricoes TO inscricoes;
GRANT CONNECT ON DATABASE coletor TO coletor;
GRANT CONNECT ON DATABASE notas TO notas;
GRANT CONNECT ON DATABASE notificacao TO notificacao;
