package org.example.artemisconnectiontest.vtbartemis.factories;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.example.artemisconnectiontest.vtbartemis.adapter.VtbArtemisSavedRequestHandler;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.stereotype.Component;

import javax.jms.ConnectionFactory;

@Component
public class ArtemisListenerFactory implements InitializingBean {
//    todo: Убрать старт компетитора
    @Autowired
    private VtbArtemisSavedRequestHandler vtbArtemisSavedRequestHandler;

    @Override
    public void afterPropertiesSet() {
        new Thread(vtbArtemisSavedRequestHandler).start();
    }

    @Bean
    public JmsListenerContainerFactory<?> ServiceFactory (ConnectionFactory service) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory ();
        factory.setConnectionFactory (service);//обязательно
        factory.setPubSubDomain (false);
        factory.setSessionTransacted (false);//обязательно, согласуется с видом подтверждений
        factory.setSessionAcknowledgeMode (ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE
                /*Session.AUTO_ACKNOWLEDGE автоматическое подтверждение перед считыванием и обработкой*/
                /*ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE требует подтверждения отдельного сообщения*/
                /*Session.CLIENT_ACKNOWLEDGE требует подтверждения после обработки группы сообщений*/
                /*ActiveMQJMSConstants.PRE_ACKNOWLEDGE эмуляция подтверждения для производительности*/);
        //не нужно для JmsListener
        //factory.setReceiveTimeout (30000L);

        return factory;
    }

}
