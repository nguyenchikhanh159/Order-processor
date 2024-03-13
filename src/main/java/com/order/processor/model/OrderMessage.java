package com.order.processor.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class OrderMessage {
    private OrderDto orderDto;
    private List<OrderDetailDto> orderDetailDtoList;
}
