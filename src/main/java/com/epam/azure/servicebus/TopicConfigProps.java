package com.epam.azure.servicebus;


import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Component
public class TopicConfigProps {
    private final String sendConnectionString;
    private final String sendEntityPath;
    private final String listenConnectionString;
    private final String listenEntityPath;

    public TopicConfigProps(
            @Value("${topic.send.connection}") String sendConnectionString,
            @Value("${topic.send.entityPath}") String sendEntityPath,

            @Value("${topic.listen.connection}") String listenConnectionString,
            @Value("${topic.listen.entityPath}") String listenEntityPath) {
        this.sendConnectionString = sendConnectionString;
        this.sendEntityPath = sendEntityPath;
        this.listenConnectionString = listenConnectionString;
        this.listenEntityPath = listenEntityPath;
    }
}
