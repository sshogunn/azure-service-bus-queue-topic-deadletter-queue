package com.epam.azure.servicebus;

import com.epam.azure.servicebus.send.EventsConsumer;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.SubscriptionClient;
import com.microsoft.azure.servicebus.TopicClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@RequiredArgsConstructor
public class TopicConfig {
    private final TopicConfigProps topicConfigProps;

    @Bean
    public TopicClient sendClient() throws ServiceBusException, InterruptedException {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.topicConfigProps.getSendConnectionString(),
                this.topicConfigProps.getSendEntityPath()
        );
        return new TopicClient(connectionStringBuilder);
    }

    @Bean
    public SubscriptionClient subscriptionClient() throws ServiceBusException, InterruptedException {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.topicConfigProps.getListenConnectionString(),
                this.topicConfigProps.getListenEntityPath()
        );
        val client = new SubscriptionClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
        val handlerOptions = new MessageHandlerOptions(5, true, Duration.ofMinutes(5));
        client.registerMessageHandler(new EventsConsumer(client), handlerOptions);
        return client;
    }
}
