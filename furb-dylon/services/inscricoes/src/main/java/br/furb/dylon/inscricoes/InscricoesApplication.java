package br.furb.dylon.inscricoes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class InscricoesApplication {
    public static void main(String[] args) {
        SpringApplication.run(InscricoesApplication.class, args);
    }
}
