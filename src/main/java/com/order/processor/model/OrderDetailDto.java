package com.order.processor.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderDetailDto {
    private int orderDetailId;
    private int orderId;
    private int ticketId;
    private int quantity;
}
