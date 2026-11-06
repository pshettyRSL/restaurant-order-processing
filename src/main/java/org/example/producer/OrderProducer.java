package org.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.events.OrderEvent;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.example.Utils.Utils.getProducerConfig;
import static org.example.Utils.Utils.parseArgs;

public class OrderProducer {
    private static final String TOPIC = "restaurant.orders.created";
    private static final ObjectMapper mapper = new ObjectMapper();

    static void main(String[] args) {
        // --- Parse CLI arguments ---
        Map<String, String> params = parseArgs(args);

        String orderId = params.get("orderId");
        String restaurantId = params.get("restaurantId");
        String customerName = params.get("customer");
        String itemsStr = params.get("items");
        List<String> items = Arrays.asList(itemsStr.split(","));

        OrderEvent event = new OrderEvent(
                orderId, restaurantId, customerName,
                items, Instant.now().toString(), "CREATED"
        );


        try (Producer<String, String> producer = new KafkaProducer<>(getProducerConfig())) {
            String json = mapper.writeValueAsString(event);

            // Key = restaurant_id  →  same restaurant always hits same partition
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, event.getRestaurantId(), json);

            System.out.printf("[PRODUCER] Sending order %s | Key: %s%n",
                    event.getOrderId(), event.getRestaurantId());

            Future<RecordMetadata> future = producer.send(record, (metadata, ex) -> {
                if (ex == null) {
                    System.out.printf("  --> SUCCESS  Partition=%d  Offset=%d%n",
                            metadata.partition(), metadata.offset());
                } else {
                    System.err.printf("  --> ERROR  %s%n", ex.getMessage());
                }
            });

            future.get(); // block until acknowledged
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
