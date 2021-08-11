package com.rbkmoney.shumaich.validator.utils;

import com.rbkmoney.shumaich.validator.domain.LogWithOffset;
import com.rbkmoney.shumaich.validator.domain.OperationRecord;
import com.rbkmoney.shumaich.validator.domain.RecordId;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FilterUtils {

    public static List<OperationRecord> filterOperationRecordByRecordId(
            List<OperationRecord> operationRecords,
            RecordId recordId) {
        return operationRecords.stream()
                .filter(operationRecord -> operationRecord.getId().equals(recordId))
                .collect(Collectors.toList());
    }

    public static List<LogWithOffset> filterProcessedLogs(List<LogWithOffset> logs, List<OperationRecord> dbRecords) {
        if (dbRecords.isEmpty() || logs == null || logs.isEmpty()) {
            return logs;
        }

        return logs.stream()
                .filter(logWithOffset -> logWithOffset.getKafkaOffset() > dbRecords.get(0).getKafkaOffset())
                .collect(Collectors.toList());
    }

}
