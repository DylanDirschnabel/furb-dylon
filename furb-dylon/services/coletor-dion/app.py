import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config
import psycopg
from flask import Flask, jsonify, request

from dion import CONFIG as DION_CONFIG, DionIndisponivel, SenhaInvalida, criar_dion

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [coletor] %(message)s")
log = logging.getLogger()

FILA_ENTRADA = os.getenv("QUEUE_ENTRADA", "coletor")
FILA_INSCRICOES = os.getenv("QUEUE_INSCRICOES", "inscricoes")
FILA_NOTAS = os.getenv("QUEUE_NOTAS", "notas")
BUCKET = os.getenv("BUCKET_SNAPSHOTS", "dylon-snapshots")
INTERVALO = int(os.getenv("COLETA_INTERVALO_S", "120"))
DSN = os.getenv("DB_DSN", "postgresql://coletor:coletor@localhost:5432/coletor")

AWS_CFG = Config(connect_timeout=3, read_timeout=13, retries={"max_attempts": 1})
sqs = boto3.client("sqs", config=AWS_CFG)
s3 = boto3.client("s3", config=AWS_CFG)
secrets = boto3.client("secretsmanager", config=AWS_CFG)
dion = criar_dion()
_urls: dict[str, str] = {}

def url_fila(nome):
    if nome not in _urls:
        _urls[nome] = sqs.get_queue_url(QueueName=nome)["QueueUrl"]
    return _urls[nome]

def envelope(tipo, correlation_id, payload):
    return {
        "messageId": str(uuid.uuid4()),
        "correlationId": str(correlation_id),
        "type": tipo,
        "version": 1,
        "occurredAt": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

def enviar(fila, env):
    sqs.send_message(QueueUrl=url_fila(fila), MessageBody=json.dumps(env))

def db():
    return psycopg.connect(DSN, autocommit=False)

def criar_tabelas():
    with db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS alunos_monitorados (
                matricula    TEXT PRIMARY KEY,
                inscricao_id UUID NOT NULL,
                secret_name  TEXT NOT NULL,
                desde        TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS mensagens_processadas (
                message_id    UUID PRIMARY KEY,
                processado_em TIMESTAMPTZ NOT NULL DEFAULT now()
            );""")

def senha(secret_name):
    return secrets.get_secret_value(SecretId=secret_name)["SecretString"]

def processar(corpo: str):
    env = json.loads(corpo)
    if env.get("Type") == "Notification":
        env = json.loads(env["Message"])
    message_id, tipo, p = env["messageId"], env["type"], env["payload"]

    with db() as c:
        if c.execute("INSERT INTO mensagens_processadas VALUES (%s) ON CONFLICT DO NOTHING",
                     (message_id,)).rowcount == 0:
            log.info("mensagem %s já processada, ignorada (idempotência)", message_id)
            return

        if tipo == "validacao.solicitada":
            validar(env["correlationId"], p)
        elif tipo == "inscricao.confirmada":
            c.execute("""INSERT INTO alunos_monitorados (matricula, inscricao_id, secret_name) VALUES (%s, %s, %s)
                         ON CONFLICT (matricula) DO UPDATE SET inscricao_id = EXCLUDED.inscricao_id,
                                                               secret_name = EXCLUDED.secret_name""",
                      (p["matricula"], p["inscricaoId"], p["secretName"]))
            log.info("[%s] passando a monitorar %s", env["correlationId"], p["matricula"])
            threading.Timer(1.0, coletar_aluno_seguro, args=(p["matricula"],)).start()
        elif tipo == "inscricao.recusada":
            pass
        else:
            raise ValueError(f"tipo inesperado: {tipo}")

def validar(correlation_id, p):
    ok, motivo = True, None
    try:
        dion.login(p["matricula"], senha(p["secretName"]))
    except SenhaInvalida:
        ok, motivo = False, "SENHA_INVALIDA"
    except DionIndisponivel as e:
        ok, motivo = False, "DION_INDISPONIVEL"
        log.warning("[%s] DION indisponível: %s", correlation_id, e)
    enviar(FILA_INSCRICOES, envelope("validacao.concluida", correlation_id,
                                     {"inscricaoId": p["inscricaoId"], "ok": ok, "motivo": motivo}))
    log.info("[%s] login de %s validado: ok=%s motivo=%s", correlation_id, p["matricula"], ok, motivo)

def consumir():
    espera = 1
    while True:
        try:
            url = url_fila(FILA_ENTRADA)
            resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=10)
            if espera > 1:
                log.info("conexão com o broker restabelecida")
            espera = 1
            for m in resp.get("Messages", []):
                try:
                    processar(m["Body"])
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
                except Exception as e:
                    log.error("falha ao processar %s (será reentregue): %s", m.get("MessageId"), e)
        except Exception as e:
            log.warning("broker indisponível, nova tentativa em %ss (%s)", espera, type(e).__name__)
            time.sleep(espera)
            espera = min(espera * 2, 15)

def coletar_aluno_seguro(matricula):
    try:
        coletar_todos(matricula)
    except Exception as e:
        log.warning("primeira coleta de %s falhou: %s", matricula, e)

def coletar_todos(so_matricula=None):
    with db() as c:
        alunos = c.execute("SELECT matricula, inscricao_id, secret_name FROM alunos_monitorados").fetchall()
    if so_matricula:
        alunos = [a for a in alunos if a[0] == so_matricula]
    for matricula, inscricao_id, secret_name in alunos:
        try:
            notas = dion.buscar_notas(matricula, senha(secret_name))
        except (DionIndisponivel, SenhaInvalida) as e:
            log.warning("[%s] coleta de %s falhou (%s), tenta no próximo ciclo", inscricao_id, matricula,
                        type(e).__name__)
            continue
        env = envelope("notas.snapshot", inscricao_id,
                       {"matricula": matricula, "coletaId": str(uuid.uuid4()), "notas": notas})
        s3.put_object(Bucket=BUCKET, Key=f"{matricula}/{env['occurredAt']}.json",
                      Body=json.dumps(env, ensure_ascii=False).encode())
        enviar(FILA_NOTAS, env)
        log.info("[%s] snapshot de %s com %d nota(s) enviado", inscricao_id, matricula, len(notas))
    return len(alunos)

def agendador():
    while True:
        time.sleep(INTERVALO)
        try:
            coletar_todos()
        except Exception as e:
            log.warning("ciclo de coleta falhou: %s", e)

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify(status="UP", servico="coletor-dion", dion=os.getenv("DION_MODE", "real"))

@app.post("/coletar")
def coletar_agora():
    return jsonify(alunos=coletar_todos()), 202

@app.route("/admin/dion", methods=["GET", "POST"])
def admin_dion():
    if request.method == "POST":
        dados = request.get_json(force=True) or {}
        if "latencia_ms" in dados:
            DION_CONFIG["latencia_ms"] = int(dados["latencia_ms"])
        if "taxa_falha" in dados:
            DION_CONFIG["taxa_falha"] = max(0.0, min(1.0, float(dados["taxa_falha"])))
        log.info("ADMIN: DION agora com latência %sms e taxa de falha %s",
                 DION_CONFIG["latencia_ms"], DION_CONFIG["taxa_falha"])
    return jsonify(modo=os.getenv("DION_MODE", "real"), **DION_CONFIG)

@app.get("/admin/estado")
def admin_estado():
    with db() as c:
        alunos = c.execute("SELECT matricula, desde FROM alunos_monitorados ORDER BY desde DESC").fetchall()
    return jsonify(alunos=[{"matricula": m, "desde": d.isoformat()} for m, d in alunos], dion=DION_CONFIG)

@app.post("/admin/reset")
def admin_reset():
    with db() as c:
        c.execute("TRUNCATE alunos_monitorados, mensagens_processadas")
    log.info("ADMIN: banco do coletor zerado")
    return jsonify(status="ok")

def iniciar():
    for tentativa in range(30):
        try:
            criar_tabelas()
            break
        except psycopg.OperationalError:
            log.info("aguardando banco (%s)", tentativa)
            time.sleep(2)
    threading.Thread(target=consumir, daemon=True, name="consumidor").start()
    threading.Thread(target=agendador, daemon=True, name="agendador").start()

iniciar()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
