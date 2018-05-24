package com.epam.azure.servicebus;

import com.epam.azure.servicebus.send.EventsConsumer;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.SubscriptionClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class TopicListenConfig {
    private final TopicConfigProps topicConfigProps;
    private final int factoriesCount;

    public TopicListenConfig(
            TopicConfigProps topicConfigProps,
            @Value("${messages.factories.count}") int factoriesCount) {
        this.topicConfigProps = topicConfigProps;
        this.factoriesCount = factoriesCount;
    }

    @Bean
    public List<SubscriptionClient> subscriptionClient() {
        val handlerOptions = new MessageHandlerOptions(300, true, Duration.ofMinutes(5));
        return IntStream.range(1, factoriesCount)
                .mapToObj(i -> {
                    val client = buildSubscriptionClient();
                    registerMessageHandler(client, handlerOptions);
                    return client;
                }).collect(Collectors.toList());
    }

    private void registerMessageHandler(SubscriptionClient client, MessageHandlerOptions handlerOptions)  {
        try {
            client.registerMessageHandler(new EventsConsumer(), handlerOptions);
        } catch (InterruptedException | ServiceBusException e) {
            throw new RuntimeException("Handler cannot be registered", e);
        }
    }

    private SubscriptionClient buildSubscriptionClient() {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.topicConfigProps.getListenConnectionString(),
                this.topicConfigProps.getListenEntityPath()
        );
        try {
            val client = new SubscriptionClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
            client.setPrefetchCount(200);
            return client;
        } catch (InterruptedException | ServiceBusException e) {
            throw new RuntimeException("Subscription client cannot be created", e);
        }
    }
}
