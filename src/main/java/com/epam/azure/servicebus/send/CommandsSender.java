package com.epam.azure.servicebus.send;

import com.epam.azure.servicebus.FakeMessage;
import com.epam.azure.servicebus.QueueSendClientPool;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.primitives.ServerBusyException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections4.ListUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CommandsSender {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Random RANDOM = new Random();
    private final int messagesCount;
    private final int batchSize;
    private final QueueSendClientPool queueSendClientPool;

    public CommandsSender(
            @Value("${messages.count}") int messagesCount,
            @Value("${messages.batch.size}") int batchSize,
            QueueSendClientPool queueSendClientPool) {
        this.messagesCount = messagesCount;
        this.batchSize = batchSize;
        this.queueSendClientPool = queueSendClientPool;
    }

    public void startProcess() {
        log.info("Started sending messages in the queue, number of messages is {}", this.messagesCount);
        sendAllMessages();
        log.info("All messages have been sent, see detailed logs to get more details");
    }

    private void sendAllMessages() {
        val transactionId = String.valueOf(RANDOM.nextInt(messagesCount));
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < this.messagesCount; i++) {
            log.debug("Sending a message {} from {}", i, this.messagesCount);
            val message = new Message(randomMessage());
            message.setContentType("application/json");
            val messageId = transactionId + " - " + Integer.toString(i);
            message.setMessageId(messageId);
            message.setTimeToLive(Duration.ofMinutes(60));
            messages.add(message);
        }
        ListUtils.partition(messages, batchSize)
                .forEach(this::sendBatch);
    }

    private void sendBatch(List<Message> messages) {
        val messagesIds = messages.stream().map(Message::getMessageId).collect(Collectors.toList());
        val queueClient = this.queueSendClientPool.sendClient();
        queueClient.sendBatchAsync(messages).exceptionally(throwable -> {
            if (throwable instanceof ServerBusyException) {
                log.warn("SERVICE IS BUSY. WAIT 5 SECONDS");
            }
            log.warn("BATCH OF COMMANDS {} CANNOT BE PROCESSED, will be recent one more time", messagesIds);
            rescheduleSendingMessage(messages, messagesIds);
            return null;
        });
    }

    private void rescheduleSendingMessage(List<Message> messages, List<String> messagesIds) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeAsync(() -> {
            sendBatch(messages);
            return null;
        }, CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS))
                .thenRun(() -> log.warn("BATCH OF COMMANDS {} IS RESCHEDULED TO BE SENT", messagesIds));
    }

    private static void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
