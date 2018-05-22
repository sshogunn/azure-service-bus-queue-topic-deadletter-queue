package com.epam.azure.servicebus.send;

import com.epam.azure.servicebus.FakeMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.QueueClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Random;

@Slf4j
@Component
public class CommandsSender {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Random RANDOM = new Random();
    private final int messagesCount;

    private final QueueClient queueClient;

    public CommandsSender(
            @Value("${messages.count}") int messagesCount,
            @Qualifier("send") QueueClient queueClient){
        this.messagesCount = messagesCount;
        this.queueClient = queueClient;
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Started sending messages in the queue, number of messages is {}", this.messagesCount);
        sendAllMessages();
        log.info("All messages have been sent, see detailed logs to get more details");
    }

    private void sendAllMessages() {
        for (int i = 0; i < this.messagesCount; i++) {
            log.debug("Sending a message {} from {}", i, this.messagesCount);
            val message = new Message(randomMessage());
            message.setContentType("application/json");
            val messageId = Integer.toString(i);
            message.setMessageId(messageId);
            message.setTimeToLive(Duration.ofMinutes(2));
            this.queueClient.sendAsync(message);
        }
    }

    private byte[] randomMessage() {
        val value = String.valueOf(RANDOM.nextInt(messagesCount));
        val fakeMessage = new FakeMessage(value);
        try {
            return OBJECT_MAPPER.writeValueAsBytes(fakeMessage);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot create fake message for sending test data", e);
        }
    }
}
