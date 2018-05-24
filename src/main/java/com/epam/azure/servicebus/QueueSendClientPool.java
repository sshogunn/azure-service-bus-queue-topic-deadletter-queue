package com.epam.azure.servicebus;

import com.microsoft.azure.servicebus.QueueClient;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class QueueSendClientPool {
    private final static List<QueueClient> CLIENTS_POOL = new ArrayList<>();
    private final QueueConfigProps queueConfigProps;
    private final int factoriesCount;

    public QueueSendClientPool(
            QueueConfigProps queueConfigProps,
            @Value("${messages.factories.count}") int factoriesCount) {
        this.queueConfigProps = queueConfigProps;
        this.factoriesCount = factoriesCount;
        initPool();
    }

    public QueueClient sendClient()  {
        int clientIndex = ThreadLocalRandom.current().nextInt(0, factoriesCount - 1);
        return CLIENTS_POOL.get(clientIndex);
    }

    private void initPool() {
        if (CLIENTS_POOL.isEmpty()) {
            val connectionStringBuilder = new ConnectionStringBuilder(
                    this.queueConfigProps.getSendConnectionString(),
                    this.queueConfigProps.getSendEntityPath()
            );
            for (int i = 0; i < factoriesCount; i++) {
                CLIENTS_POOL.add(buildClient(connectionStringBuilder));
            }
        }
    }

    private QueueClient buildClient(ConnectionStringBuilder connectionStringBuilder)  {
        try {
            return new QueueClient(connectionStringBuilder, ReceiveMode.PEEKLOCK);
        } catch (InterruptedException | ServiceBusException e) {
            throw new RuntimeException("Queue client cannot be initiated", e);
        }
    }
}
