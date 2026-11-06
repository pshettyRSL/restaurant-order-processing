package org.example.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private String orderId;
    private String restaurantId;
    private String customerName;
    private List<String> items;
    private String orderTime;
    private String orderStatus;
}
