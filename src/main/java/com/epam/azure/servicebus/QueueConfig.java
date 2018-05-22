package com.epam.azure.servicebus;

import com.epam.azure.servicebus.receive.CommandsConsumer;
import com.epam.azure.servicebus.send.DeadLetterConsumer;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.QueueClient;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.TopicClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@RequiredArgsConstructor
public class QueueConfig {
    private final TopicClient topicClient;
    private final QueueConfigProps queueConfigProps;

    @Bean(name = "send")
    public QueueClient sendClient() throws ServiceBusException, InterruptedException {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.queueConfigProps.getSendConnectionString(),
                this.queueConfigProps.getSendEntityPath()
        );
        return new QueueClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
    }

    @Bean(name = "deadLetter")
    public QueueClient deadLetterClient(@Qualifier("send") QueueClient sendClient) throws ServiceBusException, InterruptedException {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.queueConfigProps.getDeadLetterConnectionString(),
                this.queueConfigProps.getDeadLetterEntityPath()
        );
        val deadLetterClient = new QueueClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
        deadLetterClient.setPrefetchCount(50);
        val handlerOptions = new MessageHandlerOptions(5, true, Duration.ofMinutes(5));
        deadLetterClient.registerMessageHandler(new DeadLetterConsumer(sendClient, deadLetterClient), handlerOptions);
        return deadLetterClient;
    }

    @Bean(name = "listen")
    public QueueClient listenClient() throws ServiceBusException, InterruptedException {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.queueConfigProps.getListenConnectionString(),
                this.queueConfigProps.getListenEntityPath()
        );
        val queueClient = new QueueClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
        queueClient.setPrefetchCount(50);
        val handlerOptions = new MessageHandlerOptions(5, true, Duration.ofMinutes(5));
        queueClient.registerMessageHandler(new CommandsConsumer(this.topicClient), handlerOptions);
        return queueClient;
    }
}
