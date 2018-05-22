package com.epam.azure.servicebus.send;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DeadLetterConsumer implements IMessageHandler {
    private final QueueClient sendClient;
    private final QueueClient deadLetterClient;

    public DeadLetterConsumer(QueueClient sendClient, QueueClient deadLetterClient) {
        this.sendClient = sendClient;
        this.deadLetterClient = deadLetterClient;
    }

    @Override
    public CompletableFuture<Void> onMessageAsync(IMessage message) {
        return resubmitMessage(message);
    }

    @Override
    public void notifyException(Throwable exception, ExceptionPhase phase) {
        log.error("Message cannot be processed due the failure, " + phase + "-" + exception.getMessage(), exception);
    }

    private CompletableFuture<Void> resubmitMessage(IMessage message) {
        val messageId = message.getMessageId();
        try {
            log.info("A message has not been sent and it is received from dead letter queue. Message id is {}", messageId);
            val resubmitMessage = new Message(message.getBody());
            resubmitMessage.setMessageId(messageId);
            resubmitMessage.setContentType(message.getContentType());
            resubmitMessage.setTimeToLive(Duration.ofMinutes(2));
            log.info("A message with id {} will be resubmitted one more time", messageId);
            this.sendClient.send(resubmitMessage);
            return this.deadLetterClient.completeAsync(message.getLockToken());
        } catch (InterruptedException | ServiceBusException e) {
            val error = "A message with id " + messageId + " cannot be resent due errors";
            log.error(error, e);
            CompletableFuture<Void> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }
    }
}
