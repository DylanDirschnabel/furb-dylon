package br.furb.dylon.inscricoes;

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
public class ValidacaoConsumer {
    private static final Logger log = LoggerFactory.getLogger(ValidacaoConsumer.class);

    private final SqsClient sqs;
    private final Mensageria mensageria;
    private final InscricaoService service;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper json;

    @Value("${dylon.queue-entrada}") String fila;

    public ValidacaoConsumer(SqsClient sqs, Mensageria mensageria, InscricaoService service, JdbcTemplate jdbc,
                             TransactionTemplate tx, ObjectMapper json) {
        this.sqs = sqs;
        this.mensageria = mensageria;
        this.service = service;
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
                dormir(espera);
                espera = Math.min(espera * 2, 15000);
            }
        }
    }

    void processar(String corpo) throws Exception {
        JsonNode env = json.readTree(corpo);
        UUID messageId = UUID.fromString(env.get("messageId").asText());
        String tipo = env.get("type").asText();
        if (!"validacao.concluida".equals(tipo)) {
            throw new IllegalArgumentException("tipo inesperado: " + tipo);
        }
        JsonNode p = env.get("payload");
        UUID inscricaoId = UUID.fromString(p.get("inscricaoId").asText());
        boolean ok = p.get("ok").asBoolean();
        String motivo = p.hasNonNull("motivo") ? p.get("motivo").asText() : null;

        tx.executeWithoutResult(s -> {
            int novas = jdbc.update(
                    "INSERT INTO mensagens_processadas (message_id) VALUES (?) ON CONFLICT DO NOTHING", messageId);
            if (novas == 0) {
                log.info("mensagem {} já processada, ignorada (idempotência)", messageId);
                return;
            }
            service.aoConcluirValidacao(inscricaoId, ok, motivo);
        });
    }

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
