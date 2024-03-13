package com.order.processor.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class Message {
    private Map<String, String> headers;
    private String message;

    public Message(Map<String, String> headers, String message) {
        this.headers = headers;
        this.message = message;
    }
}
