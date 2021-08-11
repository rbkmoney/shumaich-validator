package com.rbkmoney.shumaich.validator.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Data
@Builder
@AllArgsConstructor
public class LogWithOffset {

    private String planId;
    private Long batchId;
    private OperationType operationType;
    private Account account;
    private Long batchHash;
    private ValidationError validationError;

    private Long kafkaOffset;

    public LogWithOffset(ConsumerRecord<Long, com.rbkmoney.damsel.shumaich.OperationLog> consumerRecord) {
        com.rbkmoney.damsel.shumaich.OperationLog operationLog = consumerRecord.value();
        this.planId = operationLog.getPlanId();
        this.batchId = operationLog.getBatchId();
        this.operationType = convertOperationType(operationLog.getOperationType());
        this.account =
                new Account(operationLog.getAccount().getId(), operationLog.getAccount().getCurrencySymbolicCode());
        this.batchHash = operationLog.getBatchHash();
        this.validationError = operationLog.getValidationError() != null
                ? convertValidationError(operationLog.getValidationError())
                : null;
        this.kafkaOffset = consumerRecord.offset();
    }

    private OperationType convertOperationType(com.rbkmoney.damsel.shumaich.OperationType operationType) {
        switch (operationType) {
            case ROLLBACK:
                return OperationType.ROLLBACK;
            case COMMIT:
                return OperationType.COMMIT;
            case HOLD:
                return OperationType.HOLD;
            default:
                throw new RuntimeException("No such OperationType");
        }
    }

    private ValidationError convertValidationError(com.rbkmoney.damsel.shumaich.ValidationError validationError) {
        switch (validationError) {
            case HOLD_NOT_EXIST:
                return ValidationError.HOLD_NOT_EXIST;
            default:
                throw new RuntimeException("No such ValidationError");
        }
    }
}
