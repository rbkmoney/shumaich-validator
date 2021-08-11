package com.rbkmoney.shumaich.validator.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Embeddable;

import java.io.Serializable;

@Data
@Embeddable
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecordId implements Serializable {

    private String planId;
    private Long batchId;
    private Long accountId;

    public RecordId(LogWithOffset logWithOffset) {
        this.planId = logWithOffset.getPlanId();
        this.batchId = logWithOffset.getBatchId();
        this.accountId = logWithOffset.getAccount().getId();
    }
}
