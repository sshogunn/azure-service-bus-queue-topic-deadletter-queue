package com.epam.azure.servicebus.send;

import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.SubscriptionClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class EventsConsumer implements IMessageHandler {
    private final SubscriptionClient subscriptionClient;

    @Override
    public CompletableFuture<Void> onMessageAsync(IMessage message) {
        return this.subscriptionClient.completeAsync(message.getLockToken());
    }

    @Override
    public void notifyException(Throwable exception, ExceptionPhase phase) {
        log.error("Message cannot be processed due failure " + phase + "-" + exception.getMessage(), exception);
    }
}
