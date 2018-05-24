package com.epam.azure.servicebus;

import com.microsoft.azure.servicebus.TopicClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class TopicSendClientPool {
    private final static List<TopicClient> CLIENTS_POOL = new ArrayList<>();
    private final TopicConfigProps topicConfigProps;
    private final int factoriesCount;

    public TopicSendClientPool(
            TopicConfigProps topicConfigProps,
            @Value("${messages.factories.count}") int factoriesCount) {
        this.topicConfigProps = topicConfigProps;
        this.factoriesCount = factoriesCount;
        initPool();
    }


    public TopicClient sendClient() {
        int clientIndex = ThreadLocalRandom.current().nextInt(0, factoriesCount - 1);
        return CLIENTS_POOL.get(clientIndex);
    }

    private void initPool() {
        if (CLIENTS_POOL.isEmpty()) {
            val connectionStringBuilder = new ConnectionStringBuilder(
                    this.topicConfigProps.getSendConnectionString(),
                    this.topicConfigProps.getSendEntityPath()
            );
            for (int i = 0; i < factoriesCount; i++) {
                CLIENTS_POOL.add(buildClient(connectionStringBuilder));
            }
        }
    }

    private TopicClient buildClient(ConnectionStringBuilder connectionStringBuilder) {
        try {
            return new TopicClient(connectionStringBuilder);
        } catch (InterruptedException | ServiceBusException e) {
            throw new RuntimeException("Queue client cannot be initiated", e);
        }
    }

}
