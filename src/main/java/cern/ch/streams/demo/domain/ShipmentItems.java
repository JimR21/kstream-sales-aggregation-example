package cern.ch.streams.demo.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShipmentItems {
    private String itemId;
    private Integer itemAmount = 0;
    private LocalDateTime periodStart;
    private LocalDateTime periodEnd;

    public ShipmentItems addOrder(Order order) {
        itemId = order.getItemId();
        itemAmount += order.getItemAmount();
        return this;
    }

    public ShipmentItems setPeriod(LocalDateTime periodStart, LocalDateTime periodEnd) {
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
        return this;
    }
}
