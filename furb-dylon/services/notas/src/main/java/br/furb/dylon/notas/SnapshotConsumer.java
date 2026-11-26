package br.furb.dylon.notas;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

@Component
public class SnapshotConsumer {
    private static final Logger log = LoggerFactory.getLogger(SnapshotConsumer.class);
    static final String REPLICA = System.getenv().getOrDefault("HOSTNAME", "local");

    private final SqsClient sqs;
    private final Mensageria mensageria;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper json;

    @Value("${dylon.queue-entrada}") String fila;
    @Value("${dylon.topic-notas}") String topicoNotas;
    @Value("${dylon.processamento-ms}") long processamentoMs;

    public SnapshotConsumer(SqsClient sqs, Mensageria mensageria, JdbcTemplate jdbc, TransactionTemplate tx,
                            ObjectMapper json) {
        this.sqs = sqs;
        this.mensageria = mensageria;
        this.jdbc = jdbc;
        this.tx = tx;
        this.json = json;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void iniciar() {
        Thread t = new Thread(this::loop, "consumidor-" + fila);
        t.setDaemon(true);
        t.start();
    }

    private void loop() {
        long espera = 1000;
        while (true) {
            try {
                String url = mensageria.urlFila(fila);
                var resp = sqs.receiveMessage(b -> b.queueUrl(url).maxNumberOfMessages(10).waitTimeSeconds(10));
                if (espera > 1000) log.info("conexão com o broker restabelecida");
                espera = 1000;
                for (Message m : resp.messages()) {
                    try {
                        processar(m.body());
                        sqs.deleteMessage(b -> b.queueUrl(url).receiptHandle(m.receiptHandle()));
                    } catch (Exception e) {
                        log.error("falha ao processar mensagem {} (será reentregue): {}", m.messageId(), e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.warn("broker indisponível, nova tentativa em {} ms ({})", espera, e.getClass().getSimpleName());
                try {
                    Thread.sleep(espera);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                espera = Math.min(espera * 2, 15000);
            }
        }
    }

    void processar(String corpo) throws Exception {
        JsonNode env = json.readTree(corpo);
        if (!"notas.snapshot".equals(env.path("type").asText())) {
            throw new IllegalArgumentException("tipo inesperado: " + env.path("type").asText());
        }
        UUID messageId = UUID.fromString(env.get("messageId").asText());
        UUID correlationId = UUID.fromString(env.get("correlationId").asText());
        Timestamp coletadoEm = Timestamp.from(OffsetDateTime.parse(env.get("occurredAt").asText()).toInstant());
        JsonNode p = env.get("payload");
        String matricula = p.get("matricula").asText();

        tx.executeWithoutResult(s -> {
            int nova = jdbc.update(
                    "INSERT INTO mensagens_processadas (message_id, processado_por) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    messageId, REPLICA);
            if (nova == 0) {
                log.info("[{}] snapshot {} já processado, ignorado (idempotência)", REPLICA, messageId);
                return;
            }
            jdbc.update("""
                    INSERT INTO coletas (matricula, coletado_em) VALUES (?, ?)
                    ON CONFLICT (matricula) DO UPDATE
                      SET coletado_em = EXCLUDED.coletado_em, processado_em = now()
                      WHERE coletas.coletado_em < EXCLUDED.coletado_em""",
                    matricula, coletadoEm);
            if (processamentoMs > 0) {
                try {
                    Thread.sleep(processamentoMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            List<Map<String, Object>> mudancas = new ArrayList<>();
            for (JsonNode n : p.get("notas")) {
                String disciplina = n.get("disciplina").asText();
                String avaliacao = n.get("avaliacao").asText();
                double valor = n.get("valor").asDouble();
                int afetadas = jdbc.update("""
                        INSERT INTO notas (matricula, disciplina, avaliacao, valor, coletado_em)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (matricula, disciplina, avaliacao) DO UPDATE
                          SET valor = EXCLUDED.valor, coletado_em = EXCLUDED.coletado_em, atualizado_em = now()
                          WHERE notas.valor IS DISTINCT FROM EXCLUDED.valor
                            AND notas.coletado_em < EXCLUDED.coletado_em""",
                        matricula, disciplina, avaliacao, valor, coletadoEm);
                if (afetadas == 1) {
                    mudancas.add(Map.of("disciplina", disciplina, "avaliacao", avaliacao, "valor", valor));
                }
            }
            if (mudancas.isEmpty()) {
                log.info("[{}] réplica {}: snapshot de {} sem novidades", correlationId, REPLICA, matricula);
                return;
            }
            mensageria.enfileirarEvento(topicoNotas, Mensageria.envelope("notas.novas", correlationId,
                    Map.of("matricula", matricula, "notas", mudancas)));
            log.info("[{}] réplica {}: {} nota(s) nova(s) para {}, evento notas.novas na outbox",
                    correlationId, REPLICA, mudancas.size(), matricula);
        });
    }
}
