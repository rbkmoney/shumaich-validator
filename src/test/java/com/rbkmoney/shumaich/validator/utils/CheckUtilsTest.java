package com.rbkmoney.shumaich.validator.utils;

import com.rbkmoney.shumaich.validator.TestData;
import com.rbkmoney.shumaich.validator.domain.LogWithOffset;
import com.rbkmoney.shumaich.validator.domain.OperationRecord;
import com.rbkmoney.shumaich.validator.domain.OperationType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CheckUtilsTest {

    @Test
    public void checksumTests() {
        final List<LogWithOffset> logsConsistent = List.of(
                TestData.logWithOffset(1L),
                TestData.logWithOffset(1L)
        );
        final List<LogWithOffset> logsInconsistent = List.of(
                TestData.logWithOffset(1L),
                TestData.logWithOffset(2L)
        );

        final List<OperationRecord> recordsConsistent = List.of(
                TestData.operationRecord(1L),
                TestData.operationRecord(1L)
        );
        final List<OperationRecord> recordsInconsistent = List.of(
                TestData.operationRecord(1L),
                TestData.operationRecord(2L)
        );

        Assert.assertTrue(CheckUtils.checksumConsistent(logsConsistent));
        Assert.assertFalse(CheckUtils.checksumConsistent(logsInconsistent));

        Assert.assertTrue(CheckUtils.checksumConsistent(recordsConsistent, logsConsistent));
        Assert.assertFalse(CheckUtils.checksumConsistent(recordsConsistent, logsInconsistent));
        Assert.assertFalse(CheckUtils.checksumConsistent(recordsInconsistent, logsConsistent));
        Assert.assertFalse(CheckUtils.checksumConsistent(recordsInconsistent, logsInconsistent));

    }

    @Test
    public void mixedOpsTest() {
        final List<LogWithOffset> commitLogs = List.of(
                TestData.logWithOffset(OperationType.COMMIT),
                TestData.logWithOffset(OperationType.COMMIT)
        );
        final List<LogWithOffset> rollbackLogs = List.of(TestData.logWithOffset(OperationType.ROLLBACK));

        final List<OperationRecord> commitsRecords = List.of(TestData.operationRecord(OperationType.COMMIT));
        final List<OperationRecord> rollbackRecords = List.of(TestData.operationRecord(OperationType.ROLLBACK));

        Assert.assertTrue(CheckUtils.finalOperationsMixed(commitsRecords, rollbackLogs));
        Assert.assertTrue(CheckUtils.finalOperationsMixed(rollbackRecords, commitLogs));
        Assert.assertFalse(CheckUtils.finalOperationsMixed(commitsRecords, commitLogs));
        Assert.assertFalse(CheckUtils.finalOperationsMixed(rollbackRecords, rollbackLogs));

    }

    @Test
    public void containsTest() {
        final List<OperationRecord> holds = List.of(
                TestData.operationRecord(OperationType.HOLD),
                TestData.operationRecord(OperationType.HOLD)
        );

        final List<OperationRecord> commits = List.of(
                TestData.operationRecord(OperationType.COMMIT),
                TestData.operationRecord(OperationType.COMMIT)
        );

        final List<OperationRecord> mixed = List.of(
                TestData.operationRecord(OperationType.HOLD),
                TestData.operationRecord(OperationType.COMMIT)
        );


        Assert.assertTrue(CheckUtils.containsFinalOp(commits));
        Assert.assertTrue(CheckUtils.containsFinalOp(mixed));
        Assert.assertFalse(CheckUtils.containsFinalOp(holds));

        Assert.assertFalse(CheckUtils.containsHold(commits));
        Assert.assertTrue(CheckUtils.containsHold(mixed));
        Assert.assertTrue(CheckUtils.containsHold(holds));
    }

}