package br.furb.dylon.notas;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NotasController {

    private final JdbcTemplate jdbc;

    public NotasController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/notas/{matricula}")
    public List<Map<String, Object>> notas(@PathVariable String matricula) {
        return jdbc.queryForList("""
                SELECT disciplina, avaliacao, valor, atualizado_em FROM notas
                WHERE matricula = ? ORDER BY disciplina, avaliacao""", matricula);
    }

    @GetMapping("/notas/{matricula}/medias")
    public List<Map<String, Object>> medias(@PathVariable String matricula) {
        return jdbc.queryForList("""
                SELECT disciplina, ROUND(AVG(valor), 2) AS media, COUNT(*) AS avaliacoes FROM notas
                WHERE matricula = ? GROUP BY disciplina ORDER BY disciplina""", matricula);
    }

    @GetMapping("/admin/estado")
    public Map<String, Object> estado() {
        return Map.of(
                "notas", jdbc.queryForList("""
                        SELECT matricula, disciplina, avaliacao, valor, coletado_em, atualizado_em FROM notas
                        ORDER BY atualizado_em DESC, matricula, disciplina, avaliacao LIMIT 200"""),
                "totalNotas", jdbc.queryForObject("SELECT COUNT(*) FROM notas", Long.class),
                "porReplica", jdbc.queryForList("""
                        SELECT COALESCE(processado_por, '?') AS replica, COUNT(*) AS snapshots
                        FROM mensagens_processadas GROUP BY 1 ORDER BY 1"""),
                "outboxPendentes", jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE publicado_em IS NULL", Long.class),
                "coletas", jdbc.queryForList("""
                        SELECT matricula, coletado_em FROM coletas
                        ORDER BY coletado_em DESC LIMIT 50"""));
    }

    @PostMapping("/admin/esquecer")
    public Map<String, Object> esquecer(@RequestBody Map<String, Object> pedido) {
        String matricula = String.valueOf(pedido.get("matricula"));
        int quantidade = Integer.parseInt(String.valueOf(pedido.getOrDefault("quantidade", 2)));
        int apagadas = jdbc.update("""
                DELETE FROM notas WHERE ctid IN (
                    SELECT ctid FROM notas WHERE matricula = ? ORDER BY random() LIMIT ?)""", matricula, quantidade);
        return Map.of("apagadas", apagadas);
    }

    @PostMapping("/admin/reset")
    public Map<String, String> reset() {
        jdbc.update("TRUNCATE notas, coletas, mensagens_processadas, outbox");
        return Map.of("status", "ok");
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "servico", "notas");
    }
}
