import json
import os
import time

import boto3
from botocore.exceptions import EndpointConnectionError

REGION = os.getenv("AWS_REGION", "us-east-1")
MAX_RECEBIMENTOS = int(os.getenv("DLQ_MAX_RECEIVE", "3"))
REMETENTE = os.getenv("SES_REMETENTE", "dylon@furb-dylon.dev")

FILAS = {"inscricoes": 10, "coletor": 60, "notas": 10, "notificacao": 10}

TOPICOS = {
    "inscricoes-eventos": ["coletor", "notificacao"],
    "notas-eventos": ["notificacao"],
}

BUCKETS = ["dylon-snapshots"]

def cliente(servico):
    return boto3.client(servico, region_name=REGION)

def esperar_ministack():
    sqs = cliente("sqs")
    for tentativa in range(60):
        try:
            sqs.list_queues()
            return
        except (EndpointConnectionError, Exception) as e:
            print(f"[init] aguardando AWS/MiniStack ({tentativa}): {type(e).__name__}")
            time.sleep(2)
    raise SystemExit("MiniStack não respondeu")

def criar_filas():
    sqs = cliente("sqs")
    arns = {}
    for nome, visibilidade in FILAS.items():
        dlq_url = sqs.create_queue(QueueName=f"{nome}-dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        url = sqs.create_queue(
            QueueName=nome,
            Attributes={
                "VisibilityTimeout": str(visibilidade),
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(MAX_RECEBIMENTOS)}),
            },
        )["QueueUrl"]
        arns[nome] = (url, sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"])
        print(f"[init] fila {nome} (+ DLQ, maxReceiveCount={MAX_RECEBIMENTOS})")
    return arns

def criar_topicos(filas):
    sns = cliente("sns")
    sqs = cliente("sqs")
    for topico, assinantes in TOPICOS.items():
        topic_arn = sns.create_topic(Name=topico)["TopicArn"]
        existentes = {s["Endpoint"] for s in sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]}
        for fila in assinantes:
            url, arn = filas[fila]
            sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": json.dumps({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "sqs:SendMessage", "Resource": arn,
                               "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}}}],
            })})
            if arn not in existentes:
                sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=arn,
                              Attributes={"RawMessageDelivery": "true"})
            print(f"[init] {topico} -> {fila}")

def criar_buckets():
    s3 = cliente("s3")
    for b in BUCKETS:
        try:
            s3.create_bucket(Bucket=b)
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        print(f"[init] bucket {b}")

def verificar_remetente():
    try:
        cliente("ses").verify_email_identity(EmailAddress=REMETENTE)
        print(f"[init] SES remetente {REMETENTE}")
    except Exception as e:
        print(f"[init] aviso SES: {e}")

if __name__ == "__main__":
    esperar_ministack()
    filas = criar_filas()
    criar_topicos(filas)
    criar_buckets()
    verificar_remetente()
    print("[init] infraestrutura pronta")
