package org.example.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.events.OrderEvent;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

public class Utils {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "restaurant.orders.created";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                map.put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        return map;
    }

    public static Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    public static Properties getConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * Starts a consumer loop. Each service only provides:
     *   - serviceName: e.g. "KITCHEN"
     *   - groupId: e.g. "kitchen-service-group"
     *   - handler: receives the ConsumerRecord and deserialized OrderEvent
     */
    public static void startConsumer(String serviceName, String groupId,
                                     BiConsumer<ConsumerRecord<String, String>, OrderEvent> handler) {
        try (Consumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps(groupId))) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("=== " + serviceName + " SERVICE STARTED (Group: " + groupId + ") ===");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> rec : records) {
                    OrderEvent order = mapper.readValue(rec.value(), OrderEvent.class);
                    handler.accept(rec, order);
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
