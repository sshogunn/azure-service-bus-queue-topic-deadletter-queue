package com.epam.azure.servicebus.send;

import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class EventsConsumer implements IMessageHandler {

    @Override
    public CompletableFuture<Void> onMessageAsync(IMessage message) {
        log.debug("Event has been received and complete, message id is {}", message.getMessageId());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void notifyException(Throwable exception, ExceptionPhase phase) {
        log.error("Message cannot be processed due failure " + phase + "-" + exception.getMessage(), exception);
    }
}
