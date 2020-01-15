package cern.ch.streams.demo.config;

import cern.ch.streams.demo.domain.Invoices;
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
public class InvoiceStreamConfig {

    Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public static final String ORDER_TOPIC = "orders";
    public static final String AGGREGATED_INVOICES_TOPIC = "aggregated_invoices";
    public static final String INVOICE_AMOUNT_STORE_NAME = "invoice_amount_store";
    public static final String WINDOWED_INVOICE_AMOUNT_STORE_NAME = "windowed_invoice_amount_store";
    public static final String WINDOWED_INVOICE_AMOUNT_SUPPRESS_NODE_NAME = "windowed_invoice_amount_suppress";

    public static final String AGGREGATED_SHIPMENT_ITEMS_TOPIC = "aggregated_shipment_items";
    public static final String SHIPMENT_ITEM_AMOUNT_STORE_NAME = "shipment_item_store";
    public static final String WINDOWED_SHIPMENT_ITEM_STORE_NAME = "windowed_shipment_item_store";
    public static final String WINDOWED_SHIPMENT_SUPPRESS_NODE_NAME = "windowed_shipment_item_suppress";

    private final Duration windowDuration;

    private final ObjectMapper mapper;
    private final Serde<String> stringSerde;
    private final Serde<Order> orderSerde;
    private final Consumed<String, Order> orderConsumed;
    private final Serde<Invoices> invoicesSerde;
    private final Produced<String, Invoices> invoicesProduced;
    private final Serde<ShipmentItems> shipmentItemsSerde;
    private final Produced<String, ShipmentItems> shipmentItemsProduced;

    public InvoiceStreamConfig(
            @Value("${app.window.duration}") Duration windowDuration,
            ObjectMapper mapper) {
        this.windowDuration = windowDuration;
        this.mapper = mapper;
        this.stringSerde = Serdes.String();
        this.orderSerde = jsonSerde(Order.class);
        this.orderConsumed = Consumed.with(stringSerde, orderSerde);
        this.invoicesSerde = jsonSerde(Invoices.class);
        this.invoicesProduced = Produced.with(stringSerde, invoicesSerde);
        this.shipmentItemsSerde = jsonSerde(ShipmentItems.class);
        this.shipmentItemsProduced = Produced.with(stringSerde, shipmentItemsSerde);
    }

//    @Bean
    public KStream<String, Invoices> kStreamInvoices(StreamsBuilder streamsBuilder) {
        logger.info("Starting Shipment Items kStream");
        var ordersByCustomerId = streamsBuilder.stream(ORDER_TOPIC, orderConsumed).groupBy(
                (key, order) -> order.getCustomerId(),
                Grouped.with(stringSerde, orderSerde));
        var aggregatedInvoicedById = aggregate(ordersByCustomerId);
        aggregatedInvoicedById.to(AGGREGATED_INVOICES_TOPIC, invoicesProduced);
        return aggregatedInvoicedById;
    }

    private Float initialize() {
        return 0f;
    }

    private Float aggregateAmount(String key, Order order, Float aggregatedAmount) {
        return aggregatedAmount;
    }

    private <T> Serde<T> jsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(
                new JsonSerializer<>(mapper),
                new JsonDeserializer<>(targetClass, mapper, false)
        );
    }

    private KStream<String, Invoices> aggregate(KGroupedStream<String, Order> ordersByCustomerId) {

        // If no window in configured then we perform a regular aggregation
        // We just store the aggregated amount in the state store
        // When we aggregate this way intermediate results will be sent to Kafka regularly
        if (windowDuration.isZero()) {
            return ordersByCustomerId
                    .aggregate(
                            Invoices::new,
                            (key, order, aggregatedInvoices) -> aggregatedInvoices.addOrder(order),
                            materializedAsPersistentStore(INVOICE_AMOUNT_STORE_NAME, stringSerde, invoicesSerde))
                    .toStream()
                    .mapValues((key, invoices) -> invoices);
        }

        // Now if we have a window configured we aggregate orders contained in a time window
        // - First we need to window the incoming records. We use a time window which is a fixed-size window.
        // - Then we aggregate the records. Here we use a different materialized that relies on a WindowStore
        // rather than a regular KeyValueStore.
        // - Here we only want the final aggregation of each period to be send to Kakfa. To do
        // that we use the suppress method and tell it to suppress all records until the window closes.
        // - Then we just need to map the key before sending to Kafka because the windowing operation changed
        // it into a windowed key. We also inject the window start and end timestamps into the final record.
        return ordersByCustomerId.windowedBy(TimeWindows.of(windowDuration).grace(Duration.ZERO))
                .aggregate(
                        Invoices::new,
                        (key, order, aggregatedInvoice) -> aggregatedInvoice.addOrder(order),
                        materializedAsWindowStore(WINDOWED_INVOICE_AMOUNT_STORE_NAME, stringSerde, invoicesSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(WINDOWED_INVOICE_AMOUNT_SUPPRESS_NODE_NAME))
                .toStream()
                .map((key, invoices) -> {
                    var start = LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault());
                    var end = LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault());
                    return KeyValue.pair(invoices.getCustomerId(), invoices.setPeriod(start, end));
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
