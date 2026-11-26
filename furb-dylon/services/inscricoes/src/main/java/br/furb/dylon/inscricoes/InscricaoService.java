package br.furb.dylon.inscricoes;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.ResourceExistsException;

@Service
public class InscricaoService {
    private static final Logger log = LoggerFactory.getLogger(InscricaoService.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Mensageria mensageria;
    private final SecretsManagerClient secrets;

    @Value("${dylon.vagas-total}") int vagasTotal;
    @Value("${dylon.validacao-timeout-s}") int timeoutS;
    @Value("${dylon.queue-coletor}") String filaColetor;
    @Value("${dylon.topic-inscricoes}") String topicoInscricoes;

    public InscricaoService(JdbcTemplate jdbc, TransactionTemplate tx, Mensageria mensageria, SecretsManagerClient secrets) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.mensageria = mensageria;
        this.secrets = secrets;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void configurarVagas() {
        jdbc.update("INSERT INTO vagas (id, total) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET total = EXCLUDED.total",
                vagasTotal);
        log.info("vagas de monitoramento: {}", vagasTotal);
    }

    public record Resultado(UUID id, String status, String motivo) {}

    public static class InscricaoAtivaException extends RuntimeException {
        public InscricaoAtivaException() {
            super("já existe uma inscrição ativa para essa matrícula");
        }
    }

    public enum Login { OK, SENHA_INVALIDA, NAO_INSCRITO, VALIDANDO }

    public Login login(String matricula, String senha) {
        var linhas = jdbc.queryForList(
                "SELECT status, secret_name FROM inscricoes WHERE matricula = ? AND status <> 'RECUSADA'", matricula);
        if (linhas.isEmpty()) return Login.NAO_INSCRITO;
        var i = linhas.get(0);
        if ("VALIDANDO".equals(i.get("status"))) return Login.VALIDANDO;
        String guardada = secrets.getSecretValue(b -> b.secretId((String) i.get("secret_name"))).secretString();
        boolean igual = java.security.MessageDigest.isEqual(
                guardada.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                senha.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return igual ? Login.OK : Login.SENHA_INVALIDA;
    }

    public Resultado inscrever(String matricula, String senha, String email) {
        UUID id = UUID.randomUUID();
        String secretName = "dylon/aluno/" + matricula;

        return tx.execute(s -> {
            Integer ativas = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM inscricoes WHERE matricula = ? AND status <> 'RECUSADA'", Integer.class, matricula);
            if (ativas != null && ativas > 0) {
                throw new InscricaoAtivaException();
            }
            boolean reservou = jdbc.update(
                    "UPDATE vagas SET ocupadas = ocupadas + 1 WHERE id = 1 AND ocupadas < total") == 1;

            if (reservou) {
                salvarSenha(secretName, senha);
                jdbc.update("""
                        INSERT INTO inscricoes (id, matricula, email, status, secret_name, prazo_validacao)
                        VALUES (?, ?, ?, 'VALIDANDO', ?, ?)""",
                        id, matricula, email, secretName, Timestamp.from(Instant.now().plusSeconds(timeoutS)));
                mensageria.enfileirarComando(filaColetor, Mensageria.envelope("validacao.solicitada", id, Map.of(
                        "inscricaoId", id.toString(), "matricula", matricula, "secretName", secretName)));
                log.info("[{}] vaga reservada, validação solicitada ao coletor", id);
                return new Resultado(id, "VALIDANDO", null);
            }

            jdbc.update("""
                    INSERT INTO inscricoes (id, matricula, email, status, motivo)
                    VALUES (?, ?, ?, 'RECUSADA', 'SEM_VAGAS')""", id, matricula, email);
            mensageria.enfileirarEvento(topicoInscricoes, Mensageria.envelope("inscricao.recusada", id, Map.of(
                    "inscricaoId", id.toString(), "matricula", matricula, "email", email, "motivo", "SEM_VAGAS")));
            log.info("[{}] sem vagas, inscrição recusada", id);
            return new Resultado(id, "RECUSADA", "SEM_VAGAS");
        });
    }

    private void salvarSenha(String nome, String senha) {
        try {
            secrets.createSecret(b -> b.name(nome).secretString(senha));
        } catch (ResourceExistsException e) {
            secrets.putSecretValue(b -> b.secretId(nome).secretString(senha));
        }
    }

    public Optional<Map<String, Object>> buscar(UUID id) {
        List<Map<String, Object>> r = jdbc.queryForList(
                "SELECT id, matricula, email, status, motivo, criado_em, atualizado_em FROM inscricoes WHERE id = ?", id);
        return r.stream().findFirst();
    }

    public Map<String, Object> vagas() {
        return jdbc.queryForMap("SELECT total, ocupadas, total - ocupadas AS livres FROM vagas WHERE id = 1");
    }

    public Map<String, Object> estado() {
        return Map.of(
                "vagas", vagas(),
                "porStatus", jdbc.queryForList(
                        "SELECT status, COALESCE(motivo, '') AS motivo, COUNT(*) AS total FROM inscricoes GROUP BY 1, 2 ORDER BY 1, 2"),
                "inscricoes", jdbc.queryForList("""
                        SELECT id, matricula, email, status, motivo, criado_em, atualizado_em
                        FROM inscricoes ORDER BY criado_em DESC LIMIT 15"""),
                "outboxPendentes", jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE publicado_em IS NULL", Long.class));
    }

    public void resetar() {
        tx.executeWithoutResult(s -> {
            jdbc.update("TRUNCATE inscricoes, mensagens_processadas, outbox");
            jdbc.update("UPDATE vagas SET ocupadas = 0 WHERE id = 1");
        });
        log.info("ADMIN: banco de inscrições zerado");
    }

    public void aoConcluirValidacao(UUID inscricaoId, boolean ok, String motivo) {
        var linhas = jdbc.queryForList(
                "SELECT matricula, email, status, secret_name FROM inscricoes WHERE id = ? FOR UPDATE", inscricaoId);
        if (linhas.isEmpty()) {
            log.warn("[{}] validação para inscrição desconhecida, ignorada", inscricaoId);
            return;
        }
        var i = linhas.get(0);
        if (!"VALIDANDO".equals(i.get("status"))) {
            log.info("[{}] resposta tardia do DION (status atual {}), descartada", inscricaoId, i.get("status"));
            return;
        }
        String matricula = (String) i.get("matricula");
        String email = (String) i.get("email");
        if (ok) {
            jdbc.update("UPDATE inscricoes SET status = 'CONFIRMADA', atualizado_em = now() WHERE id = ?", inscricaoId);
            mensageria.enfileirarEvento(topicoInscricoes, Mensageria.envelope("inscricao.confirmada", inscricaoId, Map.of(
                    "inscricaoId", inscricaoId.toString(), "matricula", matricula, "email", email,
                    "secretName", i.get("secret_name"))));
            log.info("[{}] inscrição CONFIRMADA", inscricaoId);
        } else {
            recusarCompensando(inscricaoId, matricula, email, motivo == null ? "DION_INDISPONIVEL" : motivo);
        }
    }

    private void recusarCompensando(UUID id, String matricula, String email, String motivo) {
        jdbc.update("UPDATE inscricoes SET status = 'RECUSADA', motivo = ?, atualizado_em = now() WHERE id = ?", motivo, id);
        jdbc.update("UPDATE vagas SET ocupadas = ocupadas - 1 WHERE id = 1");
        mensageria.enfileirarEvento(topicoInscricoes, Mensageria.envelope("inscricao.recusada", id, Map.of(
                "inscricaoId", id.toString(), "matricula", matricula, "email", email, "motivo", motivo)));
        log.info("[{}] COMPENSAÇÃO: inscrição recusada ({}), vaga liberada", id, motivo);
    }

    @Scheduled(fixedDelay = 2000)
    public void expirarValidacoes() {
        tx.executeWithoutResult(s -> {
            var vencidas = jdbc.queryForList("""
                    SELECT id, matricula, email FROM inscricoes
                    WHERE status = 'VALIDANDO' AND prazo_validacao < now()
                    FOR UPDATE SKIP LOCKED""");
            for (var v : vencidas) {
                recusarCompensando((UUID) v.get("id"), (String) v.get("matricula"), (String) v.get("email"), "TIMEOUT");
            }
        });
    }
}
