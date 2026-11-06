package org.example.listener;

import org.example.Utils.Utils;

public class KitchenService {
     public static void main(String[] args) {
         Utils.startConsumer("KITCHEN", "kitchen-service-group", (rec, order) ->
                 System.out.printf("[KITCHEN] Part:%d Off:%d Key:%s → Preparing %s (Order %s)%n",
                         rec.partition(), rec.offset(), rec.key(), order.getItems(), order.getOrderId())
         );
    }
}
