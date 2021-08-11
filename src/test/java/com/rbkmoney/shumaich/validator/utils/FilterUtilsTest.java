package com.rbkmoney.shumaich.validator.utils;

import com.rbkmoney.shumaich.validator.TestData;
import com.rbkmoney.shumaich.validator.domain.OperationRecord;
import com.rbkmoney.shumaich.validator.domain.RecordId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FilterUtilsTest {

    @Test
    public void operationRecordsFiltered() {
        final List<OperationRecord> operationRecords = List.of(
                TestData.operationRecord(1L, "plan1", 1L),
                TestData.operationRecord(1L, "plan1", 2L),
                TestData.operationRecord(2L, "plan1", 1L),
                TestData.operationRecord(1L, "plan2", 1L)
        );

        Assert.assertEquals(
                1,
                FilterUtils.filterOperationRecordByRecordId(operationRecords, new RecordId("plan1", 1L, 2L)).size()
        );
    }

}