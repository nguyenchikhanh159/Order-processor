package com.order.processor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectMapperHelper {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String convertObjectToString(Object o) {
        String json = "";
        if(o == null) {
            return json;
        }

        try {
             json = objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            log.error("Error when converting object to string", e);
            e.printStackTrace();
        }
        return json;
    }

    public static <T> T convertStringToObject(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            log.error("Error when converting json string to object");
            e.printStackTrace();
            return null;
        }
    }

}
