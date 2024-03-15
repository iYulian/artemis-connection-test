package org.example.artemisconnectiontest.vtbartemis.utils;

import lombok.Getter;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.example.artemisconnectiontest.utils.ReportFields;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
@Getter
public class VtbArtemisJmsUtils implements InitializingBean {
    @Autowired
    ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private ReportFields reportFields;
    private MessageProducer messageProducer;
    private MessageConsumer messageConsumer;

    @Override
    public void afterPropertiesSet() {
        //объекты JMS, использовать JmsTemplate не годится, так как это приводит к потере сообщений
        try {

            this.connection = connectionFactory.createConnection();
            this.session = connection.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);

            this.messageProducer = session.createProducer(
                    ActiveMQDestination.createDestination("DC.fbti-dev.FTI.FTI_TIM.SEARCH_CLIENT.REQ.Q",
                            ActiveMQDestination.TYPE.QUEUE));

            this.reportFields = new ReportFields("DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q:" +
                    ":DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q", "Correlation ID-", true)
                    .putReportAddress("DC.fbti-dev.FTI_TIM.IGTCLOSENOTIF.REQ.Q");

            this.messageConsumer = session.createConsumer(reportFields.getReplyToDestination());

            System.out.println("Artemis utils complete/reload!");
        } catch (JMSException e) {
            System.err.println (e);
            //оставляем только соединение
            session = null;
            messageProducer = null;
            messageConsumer = null;
            afterPropertiesSet();
        }
    }
}