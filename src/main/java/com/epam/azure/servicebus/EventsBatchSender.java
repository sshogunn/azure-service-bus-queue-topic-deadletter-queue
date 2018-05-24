package com.epam.azure.servicebus;

import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.primitives.ServerBusyException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class EventsBatchSender {
    private final List<Message> events = new ArrayList<>();
    private final TopicSendClientPool topicSendClientPool;
    private final int batchSize;

    public EventsBatchSender(
            TopicSendClientPool topicSendClientPool,
            @Value("${messages.batch.size}") int batchSize) {
        this.topicSendClientPool = topicSendClientPool;
        this.batchSize = batchSize;
    }

    public synchronized void send(Message event) {
        this.events.add(event);
        if(this.events.size() >= batchSize) {
            List<Message> toBeSent = new ArrayList<>(events);
            this.events.clear();
            this.topicSendClientPool.sendClient().sendBatchAsync(toBeSent).exceptionally(throwable -> {
                if (throwable instanceof ServerBusyException) {
                    log.warn("SERVICE IS BUSY. WAIT 5 SECONDS");
                }
                log.warn("EVENT WITH ID {} CANNOT BE PROCESSED, will be recent one more time", event.getMessageId());
                rescheduleSendingMessage(toBeSent);
                return null;
            });
        }
    }

    private void rescheduleSendingMessage(List<Message> events) {
        val messagesIds = events.stream().map(Message::getMessageId).collect(Collectors.toList());
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeAsync(() -> {
            this.topicSendClientPool.sendClient().sendBatchAsync(events);
            return null;
        }, CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS))
                .thenRun(() -> log.warn("EVENTS {} ARE RESCHEDULED TO BE SENT", messagesIds));
    }
}
