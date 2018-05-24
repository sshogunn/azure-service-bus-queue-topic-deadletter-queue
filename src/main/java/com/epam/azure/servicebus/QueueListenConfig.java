package com.epam.azure.servicebus;

import com.epam.azure.servicebus.receive.CommandsConsumer;
import com.epam.azure.servicebus.send.DeadLetterConsumer;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.QueueClient;
import com.microsoft.azure.servicebus.ReceiveMode;
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
public class QueueListenConfig {
    private final EventsBatchSender eventsBatchSender;
    private final QueueConfigProps queueConfigProps;
    private final int factoriesCount;


    public QueueListenConfig(
            EventsBatchSender eventsBatchSender,
            QueueConfigProps queueConfigProps,
            @Value("${messages.factories.count}") int factoriesCount) {
        this.eventsBatchSender = eventsBatchSender;
        this.queueConfigProps = queueConfigProps;
        this.factoriesCount = factoriesCount;
    }

    @Bean(name = "deadLetter")
    public QueueClient deadLetterClient(QueueSendClientPool queueSendClientPool) throws ServiceBusException, InterruptedException {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.queueConfigProps.getDeadLetterConnectionString(),
                this.queueConfigProps.getDeadLetterEntityPath()
        );
        val deadLetterClient = new QueueClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
        deadLetterClient.setPrefetchCount(50);
        val handlerOptions = new MessageHandlerOptions(10, true, Duration.ofMinutes(5));
        val sendClient = queueSendClientPool.sendClient();
        deadLetterClient.registerMessageHandler(new DeadLetterConsumer(sendClient), handlerOptions);
        return deadLetterClient;
    }

    @Bean
    public List<QueueClient> listenClients() {
        val handlerOptions = new MessageHandlerOptions(300, true, Duration.ofMinutes(5));
        return IntStream.range(1, factoriesCount)
                .mapToObj(i -> {
                    val queueClient = buildListenClient();
                    registerMessageHandler(handlerOptions, queueClient);
                    return queueClient;
                }).collect(Collectors.toList());
    }

    private QueueClient buildListenClient() {
        val connectionStringBuilder = new ConnectionStringBuilder(
                this.queueConfigProps.getListenConnectionString(),
                this.queueConfigProps.getListenEntityPath()
        );
        try {
            val queueClient = new QueueClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
            queueClient.setPrefetchCount(200);
            return queueClient;
        } catch (InterruptedException | ServiceBusException e) {
            throw new RuntimeException("Listen client cannot be initialized", e);
        }
    }

    private void registerMessageHandler(MessageHandlerOptions handlerOptions, QueueClient queueClient) {
        try {
            queueClient.registerMessageHandler(new CommandsConsumer(this.eventsBatchSender), handlerOptions);
        } catch (InterruptedException | ServiceBusException e) {
            throw new RuntimeException("Message handler cannot be initialized", e);
        }
    }
}
