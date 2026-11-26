package br.furb.dylon.inscricoes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

@Component
public class Mensageria {
    private static final Logger log = LoggerFactory.getLogger(Mensageria.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final SqsClient sqs;
    private final SnsClient sns;
    private final ObjectMapper json;
    private final Map<String, String> enderecos = new ConcurrentHashMap<>();

    public Mensageria(JdbcTemplate jdbc, TransactionTemplate tx, SqsClient sqs, SnsClient sns, ObjectMapper json) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.sqs = sqs;
        this.sns = sns;
        this.json = json;
    }

    public static Map<String, Object> envelope(String type, UUID correlationId, Map<String, Object> payload) {
        return Map.of(
                "messageId", UUID.randomUUID().toString(),
                "correlationId", correlationId.toString(),
                "type", type,
                "version", 1,
                "occurredAt", Instant.now().toString(),
                "payload", payload);
    }

    public void enfileirarComando(String fila, Map<String, Object> envelope) {
        gravar("sqs", fila, envelope);
    }

    public void enfileirarEvento(String topico, Map<String, Object> envelope) {
        gravar("sns", topico, envelope);
    }

    private void gravar(String tipo, String destino, Map<String, Object> envelope) {
        try {
            jdbc.update("INSERT INTO outbox (destino_tipo, destino, corpo) VALUES (?, ?, ?)",
                    tipo, destino, json.writeValueAsString(envelope));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Scheduled(fixedDelay = 500)
    public void relay() {
        try {
            tx.executeWithoutResult(s -> {
                var pendentes = jdbc.queryForList(
                        "SELECT id, destino_tipo, destino, corpo FROM outbox WHERE publicado_em IS NULL "
                                + "ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED");
                for (var linha : pendentes) {
                    publicar((String) linha.get("destino_tipo"), (String) linha.get("destino"), (String) linha.get("corpo"));
                    jdbc.update("UPDATE outbox SET publicado_em = now() WHERE id = ?", linha.get("id"));
                }
            });
        } catch (Exception e) {
            log.warn("outbox: falha ao publicar, tentando de novo em seguida ({})", e.getMessage());
        }
    }

    private void publicar(String tipo, String destino, String corpo) {
        if (tipo.equals("sqs")) {
            sqs.sendMessage(b -> b.queueUrl(urlFila(destino)).messageBody(corpo));
        } else {
            sns.publish(b -> b.topicArn(arnTopico(destino)).message(corpo));
        }
    }

    public String urlFila(String nome) {
        return enderecos.computeIfAbsent("sqs:" + nome, k -> sqs.getQueueUrl(b -> b.queueName(nome)).queueUrl());
    }

    private String arnTopico(String nome) {
        return enderecos.computeIfAbsent("sns:" + nome, k -> sns.createTopic(b -> b.name(nome)).topicArn());
    }
}
