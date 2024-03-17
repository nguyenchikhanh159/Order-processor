package com.order.processor.service.kafka;

import com.order.processor.dao.entity.Order;
import com.order.processor.dao.entity.OrderDetail;
import com.order.processor.dao.entity.Ticket;
import com.order.processor.dao.repository.OrderDetailRepository;
import com.order.processor.dao.repository.OrderRepository;
import com.order.processor.dao.repository.TicketRepository;
import com.order.processor.model.Message;
import com.order.processor.model.OrderDetailDto;
import com.order.processor.model.OrderDto;
import com.order.processor.model.OrderMessage;
import com.order.processor.utils.ObjectMapperHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.order.processor.constant.KafKaConstant.TICKET_EVENTS_TOPIC;
import static com.order.processor.constant.KafKaConstant.TICKET_ORDERS_TOPIC;

@Component
public class TicketProcessorListener extends KafkaListener {

    private final OrderRepository orderRepository;

    private final OrderDetailRepository orderDetailRepository;

    private final TicketRepository ticketRepository;

    private final KafKaSender kafKaSender;

    @Value(value = "${spring.kafka.groupId:groupId}")
    private static String groupId;

    public TicketProcessorListener(ConcurrentKafkaListenerContainerFactory<String, String> factory,
                                   OrderDetailRepository orderDetailRepository,
                                   OrderRepository orderRepository,
                                   TicketRepository ticketRepository,
                                   KafKaSender kafKaSender) {
        super(TICKET_ORDERS_TOPIC, groupId, false, factory);
        this.orderRepository = orderRepository;
        this.orderDetailRepository = orderDetailRepository;
        this.ticketRepository = ticketRepository;
        this.kafKaSender = kafKaSender;
    }

    @Override
    public void consumeRecord(Map<String, String> headers, String value) {
        OrderMessage orderMessage = ObjectMapperHelper.convertStringToObject(value, OrderMessage.class);
        if (orderMessage != null) {
            mapAndSendMessageToTicketEventAndSaveOrders(orderMessage);
        }
    }

    private void mapAndSendMessageToTicketEventAndSaveOrders(OrderMessage orderMessage) {
        OrderDto orderDto = orderMessage.getOrderDto();
        List<OrderDetailDto> orderDetailDtoList = orderMessage.getOrderDetailDtoList();

        Order order = mapperToOrderEntity(orderDto);
        List<OrderDetail> orderDetail = mapToOrderDetailsEntity(orderDetailDtoList);
        List<Ticket> tickets = findAnCalculateAvailableTicket(orderDetailDtoList);

        saveOrdersAndTicket(order, orderDetail, tickets);
        sendMessageToTicketEvent(orderMessage);
    }

    private Order mapperToOrderEntity(OrderDto orderDto) {
        return Order.builder()
                .id(orderDto.getOrderId())
                .userId(orderDto.getUserId())
                .build();
    }

    private List<OrderDetail> mapToOrderDetailsEntity(List<OrderDetailDto> orderDetailDtoList) {
        List<OrderDetail> orderDetailsEntity = new ArrayList<>();
        for (OrderDetailDto orderDetail : orderDetailDtoList) {
            OrderDetail orderDetailEntity = mapToOrderDetailEntity(orderDetail);
            orderDetailsEntity.add(orderDetailEntity);
        }
        return orderDetailsEntity;
    }


    private OrderDetail mapToOrderDetailEntity(OrderDetailDto orderDetail) {
        return OrderDetail.builder()
                .id(orderDetail.getOrderDetailId())
                .ticketId(orderDetail.getTicketId())
                .orderId(orderDetail.getOrderId())
                .quantity(orderDetail.getQuantity())
                .build();
    }

    private List<Ticket> findAnCalculateAvailableTicket(List<OrderDetailDto> orderDetailDtoList) {
        List<Ticket> tickets = new ArrayList<>();
        for(OrderDetailDto orderDetailDto : orderDetailDtoList) {
            Ticket ticket = getTicketById(orderDetailDto.getTicketId());
            ticket.setAvailability(ticket.getAvailability() -  orderDetailDto.getQuantity());
            tickets.add(ticket);
        }
        return tickets;
    }

    private Ticket getTicketById(int id) {
        return ticketRepository.findById(id).
                orElseThrow(() -> new RuntimeException("Not found ticket id: " + id));
    }

    private void saveOrdersAndTicket(Order order, List<OrderDetail> orderDetail, List<Ticket> tickets) {
        ticketRepository.saveAll(tickets);
        orderRepository.save(order);
        orderDetailRepository.saveAll(orderDetail);
    }

    private Message sendMessageToTicketEvent(OrderMessage orderMessage) {
        return kafKaSender.sendMessage(orderMessage, TICKET_EVENTS_TOPIC);
    }
}
