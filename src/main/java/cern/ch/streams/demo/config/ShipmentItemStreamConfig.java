package cern.ch.streams.demo.config;

import cern.ch.streams.demo.domain.Order;
import cern.ch.streams.demo.domain.ShipmentItems;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Configuration
@EnableKafkaStreams
public class ShipmentItemStreamConfig {

    Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public static final String ORDER_TOPIC = "orders";
    public static final String AGGREGATED_SHIPMENT_ITEMS_TOPIC = "aggregated_shipment_items";
    public static final String SHIPMENT_ITEM_AMOUNT_STORE_NAME = "shipment_item_store";
    public static final String WINDOWED_SHIPMENT_ITEM_STORE_NAME = "windowed_shipment_item_store";
    public static final String WINDOWED_SHIPMENT_SUPPRESS_NODE_NAME = "windowed_shipment_item_suppress";

    private Duration windowDuration;

    private final ObjectMapper mapper;
    private final Serde<String> stringSerde;
    private final Serde<Order> orderSerde;
    private final Consumed<String, Order> orderConsumed;
    private final Serde<ShipmentItems> shipmentItemsSerde;
    private final Produced<String, ShipmentItems> shipmentItemsProduced;

    public ShipmentItemStreamConfig(@Value("${app.window.duration}") Duration windowDuration, ObjectMapper mapper) {
        this.windowDuration = windowDuration;
        this.mapper = mapper;
        this.stringSerde = Serdes.String();
        this.orderSerde = jsonSerde(Order.class);
        this.orderConsumed = Consumed.with(stringSerde, orderSerde);
        this.shipmentItemsSerde = jsonSerde(ShipmentItems.class);
        this.shipmentItemsProduced = Produced.with(stringSerde, shipmentItemsSerde);
    }

    @Bean
    public KStream<String, ShipmentItems> kStreamShipmentItems(StreamsBuilder streamsBuilder) {
        logger.info("Starting Shipment Items kStream");
        var ordersByItemId = streamsBuilder.stream(ORDER_TOPIC, orderConsumed).groupBy(
                (key, order) -> order.getItemId(),
                Grouped.with(stringSerde, orderSerde));
        var aggregatedShipmentItemsById = aggregate(ordersByItemId);
        aggregatedShipmentItemsById.to(AGGREGATED_SHIPMENT_ITEMS_TOPIC, shipmentItemsProduced);
        return aggregatedShipmentItemsById;
    }

    private <T> Serde<T> jsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(
                new JsonSerializer<>(mapper),
                new JsonDeserializer<>(targetClass, mapper, false)
        );
    }

    private KStream<String, ShipmentItems> aggregate(KGroupedStream<String, Order> ordersByItemId) {
        if (windowDuration.isZero()) {
            return ordersByItemId
                    .aggregate(
                            ShipmentItems::new,
                            (key, order, aggregatedShipmentItems) -> aggregatedShipmentItems.addOrder(order),
                            materializedAsPersistentStore(SHIPMENT_ITEM_AMOUNT_STORE_NAME, stringSerde, shipmentItemsSerde))
                    .toStream()
                    .mapValues((key, shipmentItems) -> shipmentItems);
        }

        return ordersByItemId.windowedBy(TimeWindows.of(windowDuration).grace(Duration.ZERO))
                .aggregate(
                        ShipmentItems::new,
                        (key, order, aggregatedShipmentItems) -> aggregatedShipmentItems.addOrder(order),
                        materializedAsWindowStore(WINDOWED_SHIPMENT_ITEM_STORE_NAME, stringSerde, shipmentItemsSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(WINDOWED_SHIPMENT_SUPPRESS_NODE_NAME))
                .toStream()
                .map((key, shipmentItems) -> {
                    var start = LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault());
                    var end = LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault());
                    return KeyValue.pair(shipmentItems.getItemId(), shipmentItems.setPeriod(start, end));
                });
    }

    private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedAsPersistentStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde
    ) {
        return Materialized.<K, V>as(Stores.persistentKeyValueStore(storeName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    private <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde
    ) {
        return Materialized.<K, V>as(Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }
}
