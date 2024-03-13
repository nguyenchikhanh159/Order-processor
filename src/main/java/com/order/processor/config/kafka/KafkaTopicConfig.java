package com.order.processor.config.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.ticket.orders.topic:ticket-orders}")
    private String ticketOrdersTopic;

    @Value(value = "${spring.kafka.ticket.events.topic:ticket-events}")
    private String ticketEventsTopic;

    @Value(value = "${spring.kafka.ticket.orders.number.partitions:1}")
    private Integer numberOfPartitions;

    @Value(value = "${spring.kafka.ticket.orders.number.replicates:1}")
    private Short numberOfReplicates;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic(ticketOrdersTopic, numberOfPartitions, numberOfReplicates);
    }
}
