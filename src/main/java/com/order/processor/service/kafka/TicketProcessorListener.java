package com.order.processor.service.kafka;

import com.order.processor.dao.entity.Order;
import com.order.processor.dao.entity.OrderDetail;
import com.order.processor.dao.entity.Ticket;
import com.order.processor.dao.entity.User;
import com.order.processor.dao.repository.OrderDetailRepository;
import com.order.processor.dao.repository.OrderRepository;
import com.order.processor.dao.repository.TicketRepository;
import com.order.processor.dao.repository.UserRepository;
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

    private final UserRepository userRepository;

    private final KafKaSender kafKaSender;

    @Value(value = "${spring.kafka.groupId:groupId}")
    private static String groupId;

    public TicketProcessorListener(ConcurrentKafkaListenerContainerFactory<String, String> factory,
                                   OrderDetailRepository orderDetailRepository,
                                   OrderRepository orderRepository,
                                   TicketRepository ticketRepository,
                                   UserRepository userRepository,
                                   KafKaSender kafKaSender) {
        super(TICKET_ORDERS_TOPIC, groupId, false, factory);
        this.orderRepository = orderRepository;
        this.orderDetailRepository = orderDetailRepository;
        this.ticketRepository = ticketRepository;
        this.userRepository = userRepository;
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
        User user = userRepository.findById(orderMessage.getOrderDto().getUserId()).get();
        Order order = mapperToOrderEntity(orderMessage.getOrderDto(), user);

        List<OrderDetail> orderDetail = mapToOrderDetailsEntity(orderMessage.getOrderDetailDtoList(), order);

        saveOrdersAndTicket(order, orderDetail);
        sendMessageToTicketEvent(orderMessage);
    }

    private Order mapperToOrderEntity(OrderDto orderDto, User user) {
        return Order.builder()
                .id(orderDto.getOrderId())
                .user(user)
                .build();
    }

    private List<OrderDetail> mapToOrderDetailsEntity(List<OrderDetailDto> orderDetailDtoList, Order order) {
        List<OrderDetail> orderDetailsEntity = new ArrayList<>();
        for (OrderDetailDto orderDetail : orderDetailDtoList) {
            OrderDetail orderDetailEntity = mapToOrderDetailEntityAndSaveTicket(orderDetail, order);
            orderDetailsEntity.add(orderDetailEntity);
        }
        return orderDetailsEntity;
    }


    private OrderDetail mapToOrderDetailEntityAndSaveTicket(OrderDetailDto orderDetail, Order order) {
        Ticket ticket = ticketRepository.findById(orderDetail.getTicketId()).get();
        ticket.setAvailability(ticket.getAvailability() - orderDetail.getQuantity());
        ticketRepository.save(ticket);

        return OrderDetail.builder()
                .id(orderDetail.getOrderDetailId())
                .ticket(ticket)
                .order(order)
                .quantity(orderDetail.getQuantity())
                .build();
    }

    private void saveOrdersAndTicket(Order order, List<OrderDetail> orderDetail) {
        orderRepository.save(order);
        orderDetailRepository.saveAll(orderDetail);
    }

    private Message sendMessageToTicketEvent(OrderMessage orderMessage) {
        return kafKaSender.sendMessage(orderMessage, TICKET_EVENTS_TOPIC);
    }
}
