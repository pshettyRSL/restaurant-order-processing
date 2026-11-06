package org.example.listener;

import org.example.Utils.Utils;

public class NotificationService {
    public static void main(String[] args) {
        Utils.startConsumer("NOTIFICATION", "notification-service-group", (rec, order) ->
                System.out.printf("[NOTIFICATION] Part:%d Off:%d Key:%s → Notifying %s about order %s%n",
                        rec.partition(), rec.offset(), rec.key(), order.getCustomerName(), order.getOrderId())
        );
    }
}
