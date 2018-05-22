package com.epam.azure.servicebus.receive;

import com.microsoft.azure.servicebus.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class CommandsConsumer implements IMessageHandler {
    private final TopicClient eventSendClient;

    @Override
    public CompletableFuture<Void> onMessageAsync(IMessage message) {
        sendConfirmationEvent("Message has been received and processed, message id - " + message.getMessageId());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void notifyException(Throwable exception, ExceptionPhase phase) {
        log.error("Message cannot be processed due processing failure " + phase + "-" + exception.getMessage(), exception);
    }

    private void sendConfirmationEvent(String messageText) {
        this.eventSendClient.sendAsync(new Message(messageText));
    }
}
