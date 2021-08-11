package com.rbkmoney.shumaich.validator.config;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.kafka.common.exception.handler.SeekToCurrentWithSleepBatchErrorHandler;
import com.rbkmoney.shumaich.validator.config.properties.KafkaSslProperties;
import com.rbkmoney.shumaich.validator.kafka.serde.OperationLogDeserializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaSslProperties.class)
public class KafkaConfig {

    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${kafka.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;
    @Value("${kafka.consumer.group-id}")
    private String groupId;
    @Value("${kafka.client-id}")
    private String clientId;
    @Value("${kafka.consumer.max-poll-records}")
    private int maxPollRecords;
    @Value("${kafka.consumer.max-poll-interval-ms}")
    private int maxPollIntervalMs;
    @Value("${kafka.consumer.session-timeout-ms}")
    private int sessionTimeoutMs;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    @Bean
    public Map<String, Object> consumerConfigs(KafkaSslProperties kafkaSslProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OperationLogDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        configureSsl(props, kafkaSslProperties);
        return props;
    }

    private void configureSsl(Map<String, Object> props, KafkaSslProperties kafkaSslProperties) {
        if (kafkaSslProperties.isEnabled()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
            props.put(
                    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                    new File(kafkaSslProperties.getTrustStoreLocation()).getAbsolutePath()
            );
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaSslProperties.getTrustStorePassword());
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, kafkaSslProperties.getKeyStoreType());
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, kafkaSslProperties.getTrustStoreType());
            props.put(
                    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                    new File(kafkaSslProperties.getKeyStoreLocation()).getAbsolutePath()
            );
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafkaSslProperties.getKeyStorePassword());
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, kafkaSslProperties.getKeyPassword());
        }
    }

    @Bean
    public ConsumerFactory<Long, OperationLog> consumerFactory(KafkaSslProperties kafkaSslProperties) {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(kafkaSslProperties));
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Long, OperationLog>>
                kafkaListenerContainerFactory(ConsumerFactory<Long, OperationLog> consumerFactory) {
        return createConcurrentFactory(consumerFactory, concurrency);
    }

    private KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Long, OperationLog>>
                createConcurrentFactory(ConsumerFactory<Long, OperationLog> consumerFactory, int threadsNumber) {
        ConcurrentKafkaListenerContainerFactory<Long, OperationLog> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setBatchErrorHandler(kafkaErrorHandler());
        factory.setConcurrency(threadsNumber);
        return factory;
    }

    public BatchErrorHandler kafkaErrorHandler() {
        return new SeekToCurrentWithSleepBatchErrorHandler();
    }

}
