package org.example.artemisconnectiontest;

import org.apache.activemq.artemis.api.core.client.FailoverEventListener;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.example.artemisconnectiontest.utils.PlatzCard;
import org.example.artemisconnectiontest.utils.ReportFields;
import org.example.artemisconnectiontest.utils.RequestID;
import org.example.artemisconnectiontest.utils.ResponseTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.jms.JmsException;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsBootstrapConfiguration;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;

import javax.jms.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * Пример использования отчётов ВТБ-Артемис в сценарии запрос-ответ. Класс JMSRequestReply демонстрирует работу
 * клиента и сервиса. Пример демонстрирует применение классов:
 *  - ReportFields - создание заголовков запроса и получение отчёта
 *  - RequestID - сохранение заголовков запроса для использования в отложенных ответах, информация о клиенте
 *  - ResponseTemplate - создание заголовков ответа на основании считывания RequestID из файла/строки/стрима
 * Сервис прослушивает очередь DC.Client2.Request, получает и формирует ответы, используя RequestID
 * Сервис сохраняет идентификаторы запросов в файле. С вероятностью 50% сервис "падает" - не отвечает на запрос сразу.
 * Показано, что сервис может после "падения" считать идентификаторы из файла и сформировать запоздалые ответы.
 * Клиент прослушивает очередь DC.Client2.Report для получения отчётов (о доставке и подтверждении запроса)
 * Клиент отправляет запрос и получает ответ с использованием ReportedRequestTemplate
 * Клиент ждёт ответ 20 секунд, время рассчитано таким образом, чтобы не ждать вечно в случае ошибки на сервисе.
 */
@EnableJms
@EnableAutoConfiguration (exclude={org.springframework.boot.autoconfigure.jms.artemis.ArtemisAutoConfiguration.class})
@ComponentScan(basePackageClasses=JMSRequestReply.class)
@Configuration
@Import(value= JmsBootstrapConfiguration.class)
@Lazy
public class JMSRequestReply /*implements JmsListenerConfigurer */{
    static String  url = "(tcp://dzfbti-mqc002lk.corp.dev.vtb:61616)" +
            "?"+"sslEnabled=true&verifyHost=true" +
            "&ha=true" +
            "&retryInterval=10" +
            "&retryIntervalMultiplier=1.0" +
            "&reconnectAttempts=-1" +
            "&keyStorePath=C:/APD00.00-2207-artemis-d5-client-fbti-tim-dev-tuz.pfx"+
            "&keyStorePassword=Fbti1234567890*"+
            "&trustStorePath=C:/APD00.00-2207-artemis-d5-client-fbti-tim-dev-tuz-truststore.pfx"+
            "&trustStorePassword=Fbti1234567890*";

    static String  topology_url = "(tcp://dzfbti-mqc002lk.corp.dev.vtb:61616)" +
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
     * Единственный экземпляр фабрики соединений нужен для правильной работы балансировщика
     */
    static ActiveMQConnectionFactory cf;
    /**
     * Бин учитывает особенности кластера ВТБ-Артемис через специально настроенную фабрику соединений
     * Метод объявлен синхронным, чтобы обеспечить правильную проверку cf == null и исключить параллельное создание нескольких фабрик
     * @return Фабрика соединений на базе URL.
     */
    @Bean
    public synchronized javax.jms.ConnectionFactory ServiceConnectionFactory () {
        if (cf == null){
            cf = new ActiveMQConnectionFactory (url){
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
                        String             broker = connection.getSessionFactory ().getConnection ().getRemoteAddress ();
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
        }
        return cf;
    }

    /**
     * Создание сессии
     * @param service
     * @return
     */
    @Bean
    @Lazy
    public JmsListenerContainerFactory<?> ServiceFactory (ConnectionFactory service) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory ();
        factory.setConnectionFactory (service);//обязательно
        factory.setPubSubDomain (false);
        factory.setSessionTransacted (false);//обязательно, согласуется с видом подтверждений
        factory.setSessionAcknowledgeMode (ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE
                    /*Session.AUTO_ACKNOWLEDGE автоматическое подтверждение перед считыванием и обработкой*/
                    /*ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE требует подтверждения отдельного сообщения*/
                    /*Session.CLIENT_ACKNOWLEDGE  требует подтверждения после обработки группы сообщений*/
                    /*ActiveMQJMSConstants.PRE_ACKNOWLEDGE эмуляция подтверждения для производительности*/);
        //не нужно для JmsListener
        //factory.setReceiveTimeout (30000L);

        return factory;
    }

    //метод эмулирует сервис: получает запрос и формирует ответ
	//конкуренцию большой ставить нет смысла, так как листнер забирает 1мБ сообщений из очереди, 
	//как правило, в очереди больше нет, поэтому остальные экземпляры листнеров будут простаивать
	//если нужна конкуренция, надо уменьшать окно косюмера consumerWindowSize в URL
    @JmsListener(destination = "DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q", containerFactory = "ServiceFactory", concurrency = "1-2")
    public void service1 (Message request, Session session) throws JMSException, InterruptedException, IOException {
        RequestID id = null;
        TextMessage response;
		File temp = null;

        try {
			id = RequestID.from (request,session);
			//ВТБ-Артемис передаёт запросившую ТУЗ, что можно использовать для прикладной авторизации
			System.out.println ("Service-> Request.id: " + id);
			//получим "долговременный" идентификатор запроса
	
			if (id == null)
				return;//это неправильный запрос, его не надо обрабатывать

            //Контроль повторного получения сообщения - описать использования, применить в примерах
            //Проверку повторного получения можно делать и Message.getJMSRedelivered ()
            temp = new File (System.getProperty ("java.io.tmpdir")+"/"+ id.getid ()+".requestId");

			if (temp.exists ())//скорее всего такой файл уже есть, запрос не надо обрабатывать, транзакция откатилась
				return;//она либо уже обработана, либо будет обработана асинхронно
			
            id.to (temp);
        } catch (IOException | JMSException e) {
			e.printStackTrace ();
            return;
        }
		finally{
			//подтверждать запросы надо ПЕРЕД их длительной обработкой, 
			//запрос уже сохранился, в случае сбоя он обработается асинхронно
			//если надёжность не важна, лучше использовать AUTO_ACKNOWLEDGE
			//тип подтверждения задан в методе ServiceFactory, но здесь мы просто проверим для демонстрационных целей 
			//подтверждение при разрыве соединения может вызвать javax.jms.TransactionRolledBackException, запрос повторится
        	if (session.getAcknowledgeMode () != Session.AUTO_ACKNOWLEDGE && session.getAcknowledgeMode () != Session.SESSION_TRANSACTED)
				request.acknowledge ();//Требуется для сессий INDIVIDUAL_ACKNOWLEDGE и CLIENT_ACKNOWLEDGE
		}
        //ответ должен храниться в файле на случай сбоя
        File tempR = new File (System.getProperty ("java.io.tmpdir")+"/"+ id.getid ()+".response");

        if (tempR.exists () == false) {
            //запрос ещё не обрабатывался, так как нет файла с ответом
            //теперь можно долго обрабатывать запрос
            Thread.sleep ((long)(Math.random()*10000));
            //может не получиться
            if (Math.random() > 0.5) throw new JMSException("Something wrong!");
            //формируем "ответ" и записываем его в файл для нескольких итераций отправки ответа
            FileOutputStream o = new FileOutputStream (tempR);
            o.write (("Service response " + id.getid ()).getBytes (StandardCharsets.UTF_8));
            o.close ();
        }

        //обработка закончена, запишем результат обработки в файл
        //пытаемся отправить ответ, после долгого перерыва это может не получиться
        //важно отправить ответ в той же сессии, что получен запрос
        session.createProducer (id.getDestination ())
                .send (id.prepareResponse ((response = session.createTextMessage ("Service response " + id.getid ()))),
                        DeliveryMode.PERSISTENT, 4, 60000);

        System.out.println ("Service-> Response: " + response);

        //если отправка не удастся. что бывает после долгого перерыва,
        // можно повторить отправку из файла после обработки следующего запроса.
        //имеет смысл отправлять все сохранённые файлы, кроме
        temp.delete ();
        tempR.delete ();
    }

	static class Competitor implements Runnable{
		File temp = new File (System.getProperty ("java.io.tmpdir"));

		@Override
		public void run () {

            Session session = null;
            Connection connection = null;
            TextMessage response;

			while (true) {
				try {
					Thread.sleep (2000);
				} catch (InterruptedException e) {
					return;
				}
                //тут обрабатываем сохранённые запросы.
                //ошибка может возникнуть как на обработке, так и на отправке ответа.
                //если есть *.requestId, значит сообщение получено, если есть *.response, значит ответ не отправлен
                //если есть *.requestId и нет *.response, значит либо идёт обработка, либо она прервалась
                //если есть *.requestId и нет *.response через минуту, например, то обработка прервалась, надо повторять

				//досылаем ответы на запросы, которые не были отправлены по каким либо причинам.
				//соберём названия файлов идентификаторов запросов в массив
				File list[] = temp.listFiles (new FilenameFilter() {
					@Override
					public boolean accept (File dir, String name) {
						return name.endsWith (".requestId");
					}
				});

				if (list == null || list.length == 0)
					continue;//ничего не надо досылать

				try {//для каждого файла с идентификатором запроса
					for (File t : list) {
                        //считаем идентификатор из временного файла
                        ResponseTemplate idtemp = ResponseTemplate.from (t);

                        if (idtemp == null) {
                            t.delete ();
                            continue;
                        }
                        boolean thisWOrk = false;
                        //проверим наличие ответа
                        File responseF = new File (idtemp.getid () + ".response");

                        if (responseF.exists () == false){
                            //ответа не было, обработка сорвалась, либо ещё не завершилась
                            //проверим, что файл обрабатывается дольше 15 секунд, чтобы отсечь параллельную обработку
                            if (t.lastModified () + 15000 > System.currentTimeMillis ())
                                continue;//ещё рано, может идти параллельная обработка
                            System.out.println ("Service-> Restored request: " + idtemp);
                            //запускаем повторную обработку запроса
                            Thread.sleep ((long)(Math.random ()*10000));
                            //записываем результаты в файл
                            FileOutputStream o = new FileOutputStream (responseF);
                            o.write (("Service response " + idtemp.getid ()).getBytes (StandardCharsets.UTF_8));
                            o.close ();
                            //ставим флаг о том, что обработка была в этой нити
                            thisWOrk = true;
                        }
                        if (thisWOrk == false && responseF.lastModified () + 10000 > System.currentTimeMillis ())
                            continue;//ещё рано, может идти параллельная отправка
                        //создание постоянного соединения важно для реализации HA
                        //так как переключаются на резервный брокер с сохранением сессий
                        //только те клиенты, у которых есть действующие HA-соединения
                        //попытка установить соединение во время failover будет удачной
                        //только при наличие нескольких брокеров в URL-подключения
                        //однако это вызовет отправку ответа не через тот брокер,
                        //что может привести к потерям ответов на запросы, отправленные
                        //в очередь (то есть с ::), а не в адрес
                        //при наличии единственного брокера в URL-подключения
                        //соединение будет установлено только после failback
                        //или до failover брокера

                        if (idtemp.getBroker () != null) {//надо слать через указанный брокер
                            //используем PlatzCard для получения подключения к idtemp.getBroker ()
                            Connection next = PlatzCard.brokerConnection (cf, topology_url, null,
                                    connection, idtemp.getBroker());
                            if (next != null) {
                                if (connection != next) {
                                    if (session != null) {
                                        session.close();
                                        session = null;
                                    }
                                    if (connection != null)
                                        connection.close ();
                                    connection = next;
                                }
                            }
                        }
                        if (connection == null)
                            connection = cf.createConnection();
                        if (session == null)
                            session = connection.createSession (false, Session.AUTO_ACKNOWLEDGE);
                        // тут надо прочесть файл ответа, но в примере это делать необязательно
						//отправляем ответ с использованием идентификатора
                        session.createProducer (idtemp.getDestination ())
                                .send (idtemp.prepareResponse ((response = session.createTextMessage ("Service response " + idtemp.getid ()))),
                                        DeliveryMode.PERSISTENT, 4, 60000);

                        System.out.println ("Service-> Restored response successfully sent");
						t.delete ();
                        responseF.delete ();
					}
				} catch (Exception e) {
                    System.out.println (e);
                    session = null;
                }
			}
		}
	}
    //метод получает подтверждение о доставке сообщения (отчёты)
    @JmsListener(destination = "DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q", containerFactory = "ServiceFactory")
    public void report (Message m, Session s) throws JMSException {
        ReportFields rf = ReportFields.from (m);
        System.out.println ("Report-> " + rf);

        if (s.getAcknowledgeMode () != Session.AUTO_ACKNOWLEDGE || s.getAcknowledgeMode () != Session.SESSION_TRANSACTED)
            m.acknowledge ();//Требуется для сессий INDIVIDUAL_ACKNOWLEDGE и CLIENT_ACKNOWLEDGE

    }

    static ConfigurableApplicationContext context;
    public static void main (String[] args) throws JmsException, InterruptedException, JMSException {


//        if (args.length > 0)
//            url = args[0];//если есть первый параметр, то это должен быть URL-подключения
//
        // Launch the application
        context = SpringApplication.run (JMSRequestReply.class, args);



        cf = new ActiveMQConnectionFactory (url);


        //запуск параллельного обработчика для отправки результатов "долгой" обработки
        new Thread (new Competitor()).start();


        //объекты JMS, использовать JmsTemplate не годится, так как это приводит к потере сообщений
        Session session = null;
        Connection connection = null;
        MessageProducer producer = null;
        MessageConsumer consumer = null;
        TextMessage response = null,request = null;


        //создаём помощника формирования заголовков запроса
        ReportFields rt = new ReportFields ("DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q::DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q", "Correlation ID-",true)
                .putReportAddress ("DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q");//заставляет получать отчёты в "DC.Client2.Report"
//        DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q



        for (int i=0; i < 10; i++) try {

            System.out.print ("Client-> Request: ");

//            jmsRequest.convertAndSend ("DC.Client2.Request", "Message from VTBRequestTemplate", rt);
            if (connection == null) {
                connection = cf.createConnection ();
                connection.start ();
            }

            //создаётся сессия для отправки запросов и получения ответов клиентом
            if (session == null)
                session = connection.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);


            //отправитель подключается к очереди "DC.Client2.Request"
            if (producer == null)
                producer = session.createProducer (
                        ActiveMQDestination.createDestination ("DC.fbti-dev.FTI.FTI_TIM.UPDATE_ACCOUNT.REQ.Q",
                        ActiveMQDestination.TYPE.QUEUE));

            //Пример рассчитан только на клиентов с выделенным адресом (то есть без ::)
            //После перезапуска реализация сервиса может подключиться к другому брокеру и отправить через него ответ
            //который в случае использования получателя в другой JVM (с другой фабрикой соединений), может не дойти
            //создаётся получатель ответов из DC.Client2.Reply
            if (consumer == null)
                consumer = session.createConsumer (rt.getReplyToDestination());

//            Test test = Test.builder().additionalData(LocalDateTime.now()).build();
//            request = session.createTextMessage (test.toString());
            request = session.createTextMessage ("43g" + i);

            //каждый новый запрос идентифицируем новым уникальным значением
            rt.setCorrelationID ("Correlation ID-" + i);
            rt.prepareRequest (request);


            //отправляем ответ с использованием идентификатора
            producer.send (request, DeliveryMode.PERSISTENT,
                    4, 60000);//сообщение обрабатывается 10 секунд, в очереди может быть 6 секунд

            System.out.println(request.getText());
            //ждём ответ, делаем полезные вещи
            do {//ждать надо в цикле, использовать селектор по корреляционному идентификатору не годится, так как
                //могут прийти запоздалые ответы от долгоиграющего сервиса
                try {
                    Message message = consumer.receive(6000);
                    response = (TextMessage) message;//поставим небольшой таймаут, чтобы не ждать вечно в случае ошибки сервиса
                } catch (ClassCastException e){
                    continue;
                }
                if (response == null) {
                    //нет ответа, если это продлится дольше, надо повторить
                    System.out.println ("Client-> No response, try again.");
                    break;//новый запрос протолкнёт старые ответы
                }
                response.acknowledge ();
                //определяем, нужен ли нам этот запрос
                if (rt.isResponseSuitable (response)) {
                    //обработка ожидаемого запроса
                    System.out.println ("Clinet-> Suitable response: " + response.getText() + "\n Запрос №" + i + "\n\n\n");
                    break;//это нужный запрос, из цикла ожидания можно выйти
                }
                //неожиданный ответ, его тоже надо обрабатывать, хоть он и запоздал
                System.out.println ("Client-> Belated response, ignore: " + response);

            }
            //и так долго...
             while (true);
        }
        catch (JMSException e){
            System.out.println (e);
            //оставляем только соединение
            session = null;
            producer = null;
            consumer = null;
        }
    }
}
