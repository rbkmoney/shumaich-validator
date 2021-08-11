package com.rbkmoney.shumaich.validator;


import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;

import static com.rbkmoney.shumaich.validator.TestData.OPERATION_LOG_TOPIC;

@Slf4j
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(classes = ShumaichValidatorApplication.class)
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public abstract class IntegrationTestBase {

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer<>("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    @ClassRule
    public static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1, true, 7, OPERATION_LOG_TOPIC);

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @SneakyThrows
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of(
                            "kafka.bootstrap-servers=" + kafka.getEmbeddedKafka().getBrokersAsString(),
                            "kafka.consumer.topic=" + OPERATION_LOG_TOPIC,
                            "spring.datasource.url=" + postgres.getJdbcUrl(),
                            "spring.datasource.username=" + postgres.getUsername(),
                            "spring.datasource.password=" + postgres.getPassword(),
                            "spring.flyway.url=" + postgres.getJdbcUrl(),
                            "spring.flyway.user=" + postgres.getUsername(),
                            "spring.flyway.password=" + postgres.getPassword()
                    )
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}
