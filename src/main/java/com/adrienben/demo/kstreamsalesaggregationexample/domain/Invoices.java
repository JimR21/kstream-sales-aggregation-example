package com.adrienben.demo.kstreamsalesaggregationexample.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Invoices {
    private String customerId;
    private Float amount = 0F;
    private String itemId;
    private Integer itemAmount = 0;
    private LocalDateTime periodStart;
    private LocalDateTime periodEnd;

    public Invoices(String customerId, Float amount, String itemId, Integer itemAmount) {
        this(customerId, amount, itemId, itemAmount, null, null);
    }

    public Invoices addOrder(Order order) {
        customerId = order.getCustomerId();
        amount += order.getCost();
        itemId = order.getItemId();
        itemAmount += order.getItemAmount();
        return this;
    }

    public Invoices setPeriod(LocalDateTime periodStart, LocalDateTime periodEnd) {
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
        return this;
    }
}
