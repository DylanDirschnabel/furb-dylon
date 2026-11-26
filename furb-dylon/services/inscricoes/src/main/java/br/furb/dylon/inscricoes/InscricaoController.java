package br.furb.dylon.inscricoes;

import java.util.Map;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InscricaoController {

    private final InscricaoService service;

    public InscricaoController(InscricaoService service) {
        this.service = service;
    }

    public record Pedido(String matricula, String senha, String email) {}

    @PostMapping("/inscricoes")
    public ResponseEntity<?> inscrever(@RequestBody Pedido p) {
        if (vazio(p.matricula()) || vazio(p.senha()) || vazio(p.email())) {
            return ResponseEntity.badRequest().body(Map.of("erro", "matricula, senha e email são obrigatórios"));
        }
        try {
            var r = service.inscrever(p.matricula().trim(), p.senha(), p.email().trim());
            return ResponseEntity.accepted()
                    .header("Location", "/inscricoes/" + r.id())
                    .body(Map.of("id", r.id(), "status", r.status()));
        } catch (DuplicateKeyException | InscricaoService.InscricaoAtivaException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("erro", "já existe uma inscrição ativa para essa matrícula"));
        }
    }

    public record Credenciais(String matricula, String senha) {}

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody Credenciais c) {
        if (vazio(c.matricula()) || vazio(c.senha())) {
            return ResponseEntity.badRequest().body(Map.of("erro", "matricula e senha são obrigatórios"));
        }
        return switch (service.login(c.matricula().trim(), c.senha())) {
            case OK -> ResponseEntity.ok(Map.of("ok", true));
            case SENHA_INVALIDA -> ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("erro", "senha inválida"));
            case VALIDANDO -> ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("erro", "inscrição ainda em validação"));
            case NAO_INSCRITO -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("inscrito", false));
        };
    }

    @GetMapping("/inscricoes/{id}")
    public ResponseEntity<?> status(@PathVariable UUID id) {
        return service.buscar(id).<ResponseEntity<?>>map(i -> ResponseEntity.ok(i))
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/vagas")
    public Map<String, Object> vagas() {
        return service.vagas();
    }

    @GetMapping("/admin/estado")
    public Map<String, Object> estado() {
        return service.estado();
    }

    @PostMapping("/admin/reset")
    public Map<String, String> reset() {
        service.resetar();
        return Map.of("status", "ok");
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "servico", "inscricoes");
    }

    private static boolean vazio(String s) {
        return s == null || s.isBlank();
    }
}
