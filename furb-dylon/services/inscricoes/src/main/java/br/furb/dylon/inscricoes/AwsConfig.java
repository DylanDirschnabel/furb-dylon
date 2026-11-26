package br.furb.dylon.inscricoes;

import java.net.URI;
import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class AwsConfig {

    private static <B extends SdkClientBuilder<B, ?>> B endpoint(B builder) {
        builder.overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(13)));
        String url = System.getenv("AWS_ENDPOINT_URL");
        return (url == null || url.isBlank()) ? builder : builder.endpointOverride(URI.create(url));
    }

    private static Region regiao() {
        return Region.of(System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
    }

    @Bean
    SqsClient sqs() {
        return endpoint(SqsClient.builder().region(regiao())).build();
    }

    @Bean
    SnsClient sns() {
        return endpoint(SnsClient.builder().region(regiao())).build();
    }

    @Bean
    SecretsManagerClient secrets() {
        return endpoint(SecretsManagerClient.builder().region(regiao())).build();
    }
}
