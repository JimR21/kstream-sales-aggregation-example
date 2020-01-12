package com.adrienben.demo.kstreamsalesaggregationexample.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sale {
	private String shopId;
	private Float amount;
}
