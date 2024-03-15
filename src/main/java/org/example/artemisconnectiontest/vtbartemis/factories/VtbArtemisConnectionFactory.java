package org.example.artemisconnectiontest.vtbartemis.factories;

import lombok.Getter;
import org.apache.activemq.artemis.api.core.client.FailoverEventListener;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.jms.Connection;
import javax.jms.JMSException;

@Getter
@Component
public class VtbArtemisConnectionFactory {
    /**
     * Единственный экземпляр фабрики соединений нужен для правильной работы балансировщика
     */
    private String  url = "(tcp://dzfbti-mqc002lk.corp.dev.vtb:61616)" +
            "?"+"sslEnabled=true&verifyHost=true" +
            "&ha=true" +
            "&retryInterval=10" +
            "&retryIntervalMultiplier=1.0" +
            "&reconnectAttempts=-1" +
            "&keyStorePath=C:/APD00.00-2207-artemis-d5-client-fbti-tim-dev-tuz.pfx"+
            "&keyStorePassword=Fbti1234567890*"+
            "&trustStorePath=C:/APD00.00-2207-artemis-d5-client-fbti-tim-dev-tuz-truststore.pfx"+
            "&trustStorePassword=Fbti1234567890*";

    private String  topology_url = "(tcp://dzfbti-mqc002lk.corp.dev.vtb:61616)" +
            "?"+"sslEnabled=true&verifyHost=true" +
            "&ha=true" +
            "&retryInterval=10" +
            "&retryIntervalMultiplier=1.0" +
            "&reconnectAttempts=-1" +
            "&keyStorePath=C:/APD00.00-2207-artemis-d5-client-fbti-tim-dev-tuz.pfx"+
            "&keyStorePassword=Fbti1234567890*"+
            "&trustStorePath=C:/APD00.00-2207-artemis-d5-client-fbti-tim-dev-tuz-truststore.pfx"+
            "&trustStorePassword=Fbti1234567890*";
    //варианты балансировщика
    //org.apache.activemq.artemis.api.core.client.loadbalance.RandomConnectionLoadBalancingPolicy
    //org.apache.activemq.artemis.api.core.client.loadbalance.RandomStickyConnectionLoadBalancingPolicy
    //org.apache.activemq.artemis.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy

    /**
     * Бин учитывает особенности кластера ВТБ-Артемис через специально настроенную фабрику соединений
     * Метод объявлен синхронным, чтобы обеспечить правильную проверку cf == null и исключить параллельное создание нескольких фабрик
     * @return Фабрика соединений на базе URL.
     */
    @Bean
    public synchronized javax.jms.ConnectionFactory ServiceConnectionFactory () {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory (url){
            /**
             * Переопределённый метод close () для обеспечения единственного экземпляра фабрики
             */
            public void close() {}
            /**
             * Переопределённый метод createConnection () для установки листнера соединений
             */
            public Connection createConnection () throws JMSException{
                final ActiveMQConnection c = (ActiveMQConnection) super.createConnection ();
                String  broker = c.getSessionFactory ().getConnection ().getRemoteAddress ();
                System.out.println ("Spring-> ActiveMQConnectionFactory.createConnection () : " + broker);
                /**
                 * Установка листнера в виде объекта анонимного класса
                 */
                c.setFailoverListener (new FailoverEventListener () {
                    /**
                     * Сохраняем соединение, чтобы выводить сведения о нём в сообщениях о событиях
                     */
                    ActiveMQConnection connection = c;
                    String broker = connection.getSessionFactory ().getConnection ().getRemoteAddress ();
                    /**
                     * Переопределение метода позволяет продолжать блокирующие вызовы после разрыва соединения
                     * Метод просто заставляет блокированную нить исполнения ждать
                     * @param eventType события
                     */
                    @Override
                    public void failoverEvent (FailoverEventType eventType) {
                        switch (eventType) {
                            case FAILOVER_COMPLETED:
                                //connected = true;
                                broker = connection.getSessionFactory ().getConnection ().getRemoteAddress ();
                                break;
                            case FAILURE_DETECTED:
                            case FAILOVER_FAILED:
                            default:
                                //connected = false;
                        }
                        System.out.println ("Spring-> Event triggered :" + eventType.toString () + " " + broker);
                    }
                });

                return c;
            }
        };
        //всё что ниже, можно и лучше передавать через URL, здесь только для примера
        //выключаем неблокирующую отправку без ожидания подтверждения
        cf.setBlockOnAcknowledge (true);//false - значение по умолчанию
        //устанавливаем блокирующую отправку надёжных сообщений
        cf.setBlockOnDurableSend (true);
        //устанавливаем блокирующую отправку ненадёжных сообщений
        cf.setBlockOnNonDurableSend (true);
        //запрещаем отправку и получение без подтверждения брокером
        cf.setPreAcknowledge (false);//это значение по умолчанию
        //устанавливаем опции восстановления соединений, это важно для автоматического переключения при отказе брокера
        cf.setReconnectAttempts (-1);
        cf.setRetryInterval (100);
        cf.setRetryIntervalMultiplier (1.0);
        cf.setUseTopologyForLoadBalancing (false);//важный параметр для ограничение подключений только брокерами из URL
        //уменьшаем период проверки коннекта, это позволит быстрее восстанавливать соединения и не терять сессию
        cf.setClientFailureCheckPeriod (100);
        //большое окно подтверждения позволяет долго работать без коннекта к брокеру, можно установить в url параметром confirmationWindowSize=
        //cf.setConfirmationWindowSize (1024 * 1024 * 64);
        //окно консюмера, столько один листнер забирает из очереди, можно установить в url параметром consumerWindowSize=
        cf.setConsumerWindowSize (1024 * 1024);//это значение по умолчанию
        cf.setEnableSharedClientID (true);//несколько сессий с одним идентификатором
        cf.setClientID (System.getProperty ("sun.java.command").split (" ")[0]);
        return cf;
    }

}
