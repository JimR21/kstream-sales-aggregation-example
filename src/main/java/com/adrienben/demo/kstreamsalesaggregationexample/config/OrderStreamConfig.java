package com.adrienben.demo.kstreamsalesaggregationexample.config;

import com.adrienben.demo.kstreamsalesaggregationexample.domain.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderStreamConfig {
    public static final String ORDER_TOPIC = "orders";

    private final ObjectMapper mapper;
    private final KStream<String, Order> orderStream;
    private final Serde<String> stringSerde;
    private final Serde<Order> orderSerde;
    private final Consumed<String, Order> orderConsumed;

    public OrderStreamConfig(ObjectMapper mapper) {
        this.mapper = mapper;
        this.stringSerde = Serdes.String();
        this.orderSerde = jsonSerde(Order.class);
        this.orderConsumed = Consumed.with(stringSerde, orderSerde);
        this.orderStream = new StreamsBuilder().stream(ORDER_TOPIC, orderConsumed);
    }

    public KStream<String, Order> getOrderStream() {
        return orderStream;
    }

    private <T> Serde<T> jsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(
                new JsonSerializer<>(mapper),
                new JsonDeserializer<>(targetClass, mapper, false)
        );
    }
}
