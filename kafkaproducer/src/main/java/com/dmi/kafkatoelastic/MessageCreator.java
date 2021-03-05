package com.dmi.kafkatoelastic;

import java.util.UUID;

public class MessageCreator {
    public String getMessage() {
        return UUID.randomUUID().toString();
    }
}
