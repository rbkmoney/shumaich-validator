package com.rbkmoney.shumaich.validator;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.shumaich.validator.domain.LogWithOffset;
import com.rbkmoney.shumaich.validator.domain.OperationRecord;
import com.rbkmoney.shumaich.validator.domain.OperationType;
import com.rbkmoney.shumaich.validator.domain.RecordId;

public class TestData {

    public static final String OPERATION_LOG_TOPIC = "operation_log";

    public static final Long ACCOUNT_1 = 1L;
    public static final Long ACCOUNT_2 = 2L;

    public static final String PLAN_1 = "plan1";
    public static final String PLAN_2 = "plan2";

    public static final Long BATCH_1 = 1L;

    public static final Long HASH_1 = 111L;
    public static final Long HASH_2 = 222L;

    public static final Long KAFKA_EARLY_OFFSET = 0L;
    public static final Long KAFKA_FAR_OFFSET = 1000L;

    public static OperationLog operationLog(
            Long accountId,
            String plan,
            Long batch,
            Long batchHash,
            com.rbkmoney.damsel.shumaich.OperationType operationType) {
        OperationLog operationLog = new OperationLog()
                .setAccount(new com.rbkmoney.damsel.shumaich.Account(accountId, "RUB"))
                .setPlanId(plan)
                .setBatchId(batch)
                .setBatchHash(batchHash)
                .setOperationType(operationType)
                .setDescription("test")
                .setCurrencySymbolicCode("RUB")
                .setAmountWithSign(12)
                .setCreationTimeMs(123)
                .setPlanOperationsCount(123)
                .setSequenceId(123)
                .setValidationError(null)
                .setParentId("test")
                .setSpanId("test")
                .setTraceId("test");

        return operationLog;
    }

    public static LogWithOffset logWithOffset(Long batchHash) {
        return LogWithOffset.builder()
                .batchHash(batchHash)
                .build();
    }

    public static LogWithOffset logWithOffset(OperationType operationType) {
        return LogWithOffset.builder()
                .operationType(operationType)
                .build();
    }

    public static OperationRecord operationRecord(
            Long accountId,
            String planId,
            Long batchId,
            Long batchHash,
            OperationType operationType,
            Long offset) {
        return new OperationRecord(new RecordId(planId, batchId, accountId), operationType, batchHash, offset);
    }

    public static OperationRecord operationRecord(Long accountId, String planId, Long batchId) {
        return new OperationRecord(new RecordId(planId, batchId, accountId), null, null, null);
    }

    public static OperationRecord operationRecord(OperationType operationType) {
        return new OperationRecord(null, operationType, null, null);
    }

    public static OperationRecord operationRecord(Long batchHash) {
        return new OperationRecord(null, null, batchHash, null);
    }

}
