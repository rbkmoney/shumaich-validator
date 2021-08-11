package com.rbkmoney.shumaich.validator.utils;

import com.rbkmoney.shumaich.validator.domain.LogWithOffset;
import com.rbkmoney.shumaich.validator.domain.OperationRecord;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Stream;

import static com.rbkmoney.shumaich.validator.domain.OperationType.HOLD;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CheckUtils {

    public static boolean finalOperationsMixed(List<OperationRecord> operationRecords, List<LogWithOffset> finalOps) {
        return Stream.concat(
                finalOps.stream().map(LogWithOffset::getOperationType),
                operationRecords.stream().map(OperationRecord::getOperationType)
        )
                       .filter(operationType -> !operationType.equals(HOLD))
                       .distinct()
                       .count() == 2;
    }

    public static boolean containsFinalOp(List<OperationRecord> dbRecords) {
        return dbRecords.stream().anyMatch(operationRecord -> !operationRecord.getOperationType().equals(HOLD));
    }

    public static boolean containsHold(List<OperationRecord> dbRecords) {
        return dbRecords.stream().anyMatch(operationRecord -> operationRecord.getOperationType().equals(HOLD));
    }

    public static boolean checksumConsistent(List<LogWithOffset> logs) {
        return logs.stream().map(LogWithOffset::getBatchHash).distinct().count() == 1;
    }

    public static boolean checksumConsistent(List<OperationRecord> records, List<LogWithOffset> logs) {
        return Stream.concat(
                logs.stream().map(LogWithOffset::getBatchHash),
                records.stream().map(OperationRecord::getBatchHash)
        ).distinct().count() == 1;
    }


}
