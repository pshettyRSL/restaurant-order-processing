package org.example.listener;

import org.example.Utils.Utils;

public class InventoryService {
    public static void main(String[] args) {
        Utils.startConsumer("INVENTORY", "inventory-service-group", (rec, order) ->
                System.out.printf("[INVENTORY] Part:%d Off:%d Key:%s - Deducting stock for %s%n",
                        rec.partition(), rec.offset(), rec.key(), order.getItems())
        );
    }
}
