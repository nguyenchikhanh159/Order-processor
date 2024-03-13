package com.order.processor.service.kafka;

import com.order.processor.model.Message;
import com.order.processor.utils.ObjectMapperHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.order.processor.constant.KafKaConstant.REQUEST_ID;
import static java.lang.System.currentTimeMillis;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@Service
@Slf4j
public class KafKaSender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafKaSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Message sendMessage(Object o, String topic) {
        String message = ObjectMapperHelper.convertObjectToString(o);
        return sendMessage(message, topic);
    }

    public Message sendMessage(String message, String topic) {
        long startTime = currentTimeMillis();
        log.info("Kafka message starting: Send message to destination topic: {} ", topic);
        Map<String, String> headers = createHeader(topic);
        ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, message);
        headers.forEach((key, value) -> producerRecord.headers().add(key, value.getBytes()));
        kafkaTemplate.send(producerRecord);
        log.info("Kafka message finishing: Finished to send message to destination topic: {} with time {}ms ", topic, currentTimeMillis() - startTime);
        return new Message(headers, message);
    }

    private Map<String, String> createHeader(String topic) {
        UUID id = UUID.randomUUID();
        Map<String, String> headers = new HashMap<>();
        headers.put(TOPIC, topic);
        headers.put(REQUEST_ID, id.toString());
        return headers;
    }
}
