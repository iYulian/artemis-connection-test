package org.example.artemisconnectiontest.vtbartemis.adapter;

import org.example.artemisconnectiontest.vtbartemis.utils.Test;
import org.example.artemisconnectiontest.utils.ReportFields;
import org.example.artemisconnectiontest.vtbartemis.utils.VtbArtemisJmsUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.time.LocalDateTime;

@Component
public class VtbArtemisSender {
    @Autowired
    private VtbArtemisJmsUtils jmsUtils;

    public void send() throws JMSException {

        for (int i = 0; i < 5; i++) {

            //создаём помощника формирования заголовков запроса
            ReportFields rt = jmsUtils.getReportFields();

            jmsUtils.getConnection().start();

            Test test = Test.builder().additionalData(LocalDateTime.now()).build();
            TextMessage textMessage = jmsUtils.getSession().createTextMessage(test.toString());

            //каждый новый запрос идентифицируем новым уникальным значением
            rt.setCorrelationID("Correlation ID-" + i);
            rt.prepareRequest(textMessage);

            jmsUtils.getMessageProducer().send(textMessage, DeliveryMode.PERSISTENT,
                    4, 60000);//сообщение обрабатывается 10 секунд, в очереди может быть 6 секунд

            System.out.println("Сообщение отправленное сервису :\n" + textMessage.getText());

            VtbArtemisReceiver artemisReceiver = new VtbArtemisReceiver();
            artemisReceiver.receiveMassage(jmsUtils.getMessageConsumer(), rt);
        }
    }
}
