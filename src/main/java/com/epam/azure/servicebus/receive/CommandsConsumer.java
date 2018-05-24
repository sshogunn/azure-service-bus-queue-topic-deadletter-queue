package com.epam.azure.servicebus.receive;

import com.epam.azure.servicebus.EventsBatchSender;
import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class CommandsConsumer implements IMessageHandler {
    private final EventsBatchSender eventsBatchSender;

    @Override
    public CompletableFuture<Void> onMessageAsync(IMessage message) {
        val messageId = message.getMessageId();
        log.debug("Command has been received and completed with message id {}", messageId);
        sendConfirmationEvent("Message has been received and processed, message id - " + messageId, messageId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void notifyException(Throwable exception, ExceptionPhase phase) {
        log.error("Message cannot be processed due processing failure " + phase + "-" + exception.getMessage(), exception);
    }

    private void sendConfirmationEvent(String messageText, String messagesId) {
        val message = new Message(messageText);
        message.setMessageId(messagesId);
        this.eventsBatchSender.send(message);
    }
}
