package com.rbkmoney.shumaich.validator;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.damsel.shumaich.OperationType;
import com.rbkmoney.shumaich.validator.domain.FailureReason;
import com.rbkmoney.shumaich.validator.domain.FailureRecord;
import com.rbkmoney.shumaich.validator.domain.OperationRecord;
import com.rbkmoney.shumaich.validator.domain.RecordId;
import com.rbkmoney.shumaich.validator.kafka.handler.OperationLogHandler;
import com.rbkmoney.shumaich.validator.kafka.serde.OperationLogSerializer;
import com.rbkmoney.shumaich.validator.repo.FailureRecordRepo;
import com.rbkmoney.shumaich.validator.repo.OperationRecordRepo;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.rbkmoney.shumaich.validator.TestData.*;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumaichValidatorApplication.class)
public class ShumaichValidatorIntegrationTest extends IntegrationTestBase {

    @SpyBean
    OperationLogHandler handler;

    @Autowired
    OperationRecordRepo operationRecordRepo;

    @Autowired
    FailureRecordRepo failureRecordRepo;

    private AtomicLong increment = new AtomicLong(0);

    @After
    public void clear() {
        operationRecordRepo.deleteAll();
        failureRecordRepo.deleteAll();
    }

    @Test
    public void emptyDb_Holds() {

        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(com.rbkmoney.shumaich.validator.domain.OperationType.HOLD, records.get(0).getOperationType());
        });

    }

    @Test
    public void emptyDb_HoldsWithInconsistentChecksum() {
        //given
        givenInconsistent(PLAN_1, BATCH_1, ACCOUNT_1, OperationType.HOLD);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(com.rbkmoney.shumaich.validator.domain.OperationType.HOLD, records.get(0).getOperationType());


            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(FailureReason.DIFFERENT_OPERATION_ALREADY_EXISTS, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void emptyDb_FinalOps() {
        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(FailureReason.HOLD_DOES_NOT_EXIST, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void emptyDb_HoldsAndFinalOps() {
        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
        });

    }


    @Test
    public void emptyDb_HoldsAndFinalOpsMixed() {
        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.ROLLBACK);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.FINAL_OPERATIONS_MIXED, failedRecords.get(0).getReason());
        });

    }

    @Test
    public void emptyDb_HoldsAndFinalOpsInconsistentChecksum() {
        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_2, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(com.rbkmoney.shumaich.validator.domain.OperationType.HOLD, records.get(0).getOperationType());
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.DIFFERENT_OPERATION_ALREADY_EXISTS, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void holdsInDb_Holds() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(com.rbkmoney.shumaich.validator.domain.OperationType.HOLD, records.get(0).getOperationType());
            assertEquals(HASH_1, records.get(0).getBatchHash());
        });
    }

    @Test
    public void holdsInDb_HoldsInconsistentChecksum() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_2, OperationType.HOLD);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(com.rbkmoney.shumaich.validator.domain.OperationType.HOLD, records.get(0).getOperationType());
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.DIFFERENT_OPERATION_ALREADY_EXISTS, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void holdsInDb_FinalOps() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.ROLLBACK);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.ROLLBACK,
                    records.get(0).getOperationType()
            );
            assertEquals(HASH_1, records.get(0).getBatchHash());
        });
    }

    @Test
    public void holdsInDb_FinalOpsInconsistentChecksum() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_2, OperationType.ROLLBACK);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(com.rbkmoney.shumaich.validator.domain.OperationType.HOLD, records.get(0).getOperationType());
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.DIFFERENT_OPERATION_ALREADY_EXISTS, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void finalOpInDb_Holds() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.HOLD_AFTER_FINAL_OPERATION, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void finalOpInDb_FinalOps() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(HASH_1, records.get(0).getBatchHash());
        });
    }

    @Test
    public void finalOpInDb_FinalOpsChecksumInconsistent() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_2, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.DIFFERENT_OPERATION_ALREADY_EXISTS, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void finalOpInDb_FinalOpsMixedOperationTypes() {
        //given
        givenDb(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.ROLLBACK);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.FINAL_OPERATIONS_MIXED, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void failureAlreadyExist() {
        //given
        finalOpInDb_FinalOpsMixedOperationTypes();

        //when
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_2, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(HASH_1, records.get(0).getBatchHash());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(1, failedRecords.size());
            assertEquals(new RecordId(PLAN_1, BATCH_1, ACCOUNT_1), failedRecords.get(0).getId());
            assertEquals(FailureReason.DIFFERENT_OPERATION_ALREADY_EXISTS, failedRecords.get(0).getReason());
        });
    }

    @Test
    public void severalPlans() {
        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);
        given(PLAN_2, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_2, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(2, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(1).getOperationType()
            );

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(0, failedRecords.size());
        });
    }

    @Test
    public void severalAccs() {
        //given
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_2, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);
        given(PLAN_1, BATCH_1, ACCOUNT_2, HASH_1, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(2, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(1).getOperationType()
            );

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(0, failedRecords.size());
        });
    }

    @Test
    public void offsetAlreadyRead() {
        givenDb(
                PLAN_1,
                BATCH_1,
                ACCOUNT_1,
                HASH_1,
                com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                KAFKA_FAR_OFFSET
        );
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.HOLD);
        given(PLAN_1, BATCH_1, ACCOUNT_1, HASH_1, OperationType.COMMIT);

        //then
        await().untilAsserted(() -> {
            final List<OperationRecord> records = operationRecordRepo.findAll();
            assertEquals(1, records.size());
            assertEquals(
                    com.rbkmoney.shumaich.validator.domain.OperationType.COMMIT,
                    records.get(0).getOperationType()
            );
            assertEquals(KAFKA_FAR_OFFSET, records.get(0).getKafkaOffset());

            final List<FailureRecord> failedRecords = failureRecordRepo.findAll();
            assertEquals(0, failedRecords.size());
        });

    }

    private void given(String plan, Long batch, Long account, Long batchHash, OperationType operationType) {
        var producer = producer();
        List.of(
                TestData.operationLog(account, plan, batch, batchHash, operationType),
                TestData.operationLog(account, plan, batch, batchHash, operationType),
                TestData.operationLog(account, plan, batch, batchHash, operationType)
        ).forEach(
                operationLog -> producer.sendDefault(operationLog.getAccount().getId(), operationLog)
        );

    }

    private void givenDb(
            String plan,
            Long batch,
            Long account,
            Long batchHash,
            com.rbkmoney.shumaich.validator.domain.OperationType operationType) {
        operationRecordRepo.save(TestData.operationRecord(
                account,
                plan,
                batch,
                batchHash,
                operationType,
                KAFKA_EARLY_OFFSET
        ));
    }

    private void givenDb(
            String plan,
            Long batch,
            Long account,
            Long batchHash,
            com.rbkmoney.shumaich.validator.domain.OperationType operationType,
            Long offset) {
        operationRecordRepo.save(TestData.operationRecord(account, plan, batch, batchHash, operationType, offset));
    }

    private void givenInconsistent(String plan, Long batch, Long account, OperationType operationType) {
        var producer = producer();
        List.of(
                TestData.operationLog(account, plan, batch, HASH_1, operationType),
                TestData.operationLog(account, plan, batch, HASH_1, operationType),
                TestData.operationLog(account, plan, batch, HASH_2, operationType)
        ).forEach(
                operationLog -> producer.sendDefault(operationLog.getAccount().getId(), operationLog)
        );

    }

    public KafkaTemplate<Long, OperationLog> producer() {
        Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getEmbeddedKafka());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OperationLogSerializer.class);
        KafkaTemplate<Long, OperationLog> kafkaTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(configs), true);
        kafkaTemplate.setDefaultTopic(OPERATION_LOG_TOPIC);
        return kafkaTemplate;
    }

}
