package com.epam.azure.servicebus;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Component
public class QueueConfigProps {
    private final String sendConnectionString;
    private final String sendEntityPath;

    private final String listenConnectionString;
    private final String listenEntityPath;

    private final String deadLetterConnectionString;
    private final String deadLetterEntityPath;

    public QueueConfigProps(
            @Value("${queue.send.connection}") String sendConnectionString,
            @Value("${queue.send.entityPath}") String sendEntityPath,

            @Value("${queue.listen.connection}") String listenConnectionString,
            @Value("${queue.listen.entityPath}") String listenEntityPath,

            @Value("${queue.deadLetter.connection}") String deadLetterConnectionString,
            @Value("${queue.deadLetter.entityPath}") String deadLetterEntityPath) {
        this.sendConnectionString = sendConnectionString;
        this.sendEntityPath = sendEntityPath;

        this.listenConnectionString = listenConnectionString;
        this.listenEntityPath = listenEntityPath;

        this.deadLetterConnectionString = deadLetterConnectionString;
        this.deadLetterEntityPath = deadLetterEntityPath;
    }
}
