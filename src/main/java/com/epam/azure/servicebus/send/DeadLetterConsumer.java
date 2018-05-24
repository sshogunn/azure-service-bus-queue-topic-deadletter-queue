package com.epam.azure.servicebus.send;

import com.microsoft.azure.servicebus.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DeadLetterConsumer implements IMessageHandler {
    private final QueueClient sendClient;

    public DeadLetterConsumer(QueueClient sendClient) {
        this.sendClient = sendClient;
    }

    @Override
    public CompletableFuture<Void> onMessageAsync(IMessage message) {
        log.debug("Received a command with from Dead Letter queue with message id {}", message.getMessageId());
        return resubmitMessage(message);
    }

    @Override
    public void notifyException(Throwable exception, ExceptionPhase phase) {
        log.error("Message cannot be processed due the failure, " + phase + "-" + exception.getMessage(), exception);
    }

    private CompletableFuture<Void> resubmitMessage(IMessage message) {
        val messageId = message.getMessageId();
        log.info("A message has not been sent and it is received from dead letter queue. Message id is {}", messageId);
        val resubmitMessage = new Message(message.getBody());
        resubmitMessage.setMessageId(messageId);
        resubmitMessage.setContentType(message.getContentType());
        resubmitMessage.setTimeToLive(Duration.ofMinutes(2));
        log.info("A message with id {} will be resubmitted one more time", messageId);
        resubmit(resubmitMessage);
        return CompletableFuture.completedFuture(null);
    }

    private void resubmit(Message message) {
        this.sendClient.sendAsync(message).exceptionally(throwable -> {
            log.warn("COMMAND WITH ID {} CANNOT BE PROCESSED BY DEAD LETTER CONSUMER, will be recent one more time", message.getMessageId());
            resubmit(message);
            return null;
        });
    }
}
