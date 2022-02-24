package com.example.kafka;

import java.util.Calendar;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TransactionEvent {
    @JsonProperty
    private String trxCode;
    @JsonProperty
    private String trxAcct;
    @JsonProperty
    private String trxAmount;
    @JsonProperty
    private Long sysDateString;

    public TransactionEvent() {
    }

    public TransactionEvent(String trxCode, String trxAcct, String trxAmount) {
        this.trxCode = trxCode;
        this.trxAcct = trxAcct;
        this.trxAmount = trxAmount;
        this.sysDateString = Calendar.getInstance().getTimeInMillis();
    }


}
