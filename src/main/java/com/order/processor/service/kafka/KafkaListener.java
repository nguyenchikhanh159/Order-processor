package com.order.processor.service.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.HashMap;
import java.util.Map;


public abstract class KafkaListener implements MessageListener<String, String> {

    private final String topic;
    private final String groupId;
    private final boolean autoStartup;
    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;

    public KafkaListener(String topic,
                         String groupId,
                         boolean autoStartup,
                         ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        this.topic = topic;
        this.groupId = groupId;
        this.autoStartup = autoStartup;
        this.factory = factory;
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
        String value = data.value();
        Map<String, String> headers = new HashMap<>();
        consumeRecord(headers, value);
    }

    public abstract void consumeRecord(Map<String,String> headers, String value);

    @EventListener(ApplicationReadyEvent.class)
    public ConcurrentMessageListenerContainer<String, String> createContainer() {
        ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topic);
        container.getContainerProperties().setMessageListener(this);
        container.getContainerProperties().setGroupId(groupId);
        container.setAutoStartup(autoStartup);
        container.setBeanName(groupId);
        container.start();
        return container;
    }
}
