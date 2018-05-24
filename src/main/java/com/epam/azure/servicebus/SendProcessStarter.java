package com.epam.azure.servicebus;

import com.epam.azure.servicebus.send.CommandsSender;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SendProcessStarter implements InitializingBean {
    private final CommandsSender commandsSender;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.commandsSender.startProcess();
    }
}
