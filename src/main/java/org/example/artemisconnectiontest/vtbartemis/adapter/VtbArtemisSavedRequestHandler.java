package org.example.artemisconnectiontest.vtbartemis.adapter;

import org.example.artemisconnectiontest.utils.PlatzCard;
import org.example.artemisconnectiontest.utils.ResponseTemplate;
import org.example.artemisconnectiontest.vtbartemis.utils.VtbArtemisJmsUtils;
import org.example.artemisconnectiontest.vtbartemis.factories.VtbArtemisConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.nio.charset.StandardCharsets;

@Component
public class VtbArtemisSavedRequestHandler implements Runnable {
    @Autowired
    private VtbArtemisJmsUtils artemisJmsUtils;
    @Autowired
    private VtbArtemisConnectionFactory connectionFactory;
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

            if (list == null || list.length == 0) {
                System.out.println("Thread working: Нечего досылать");
                continue;//ничего не надо досылать
            }

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
                        Connection next = PlatzCard.brokerConnection (artemisJmsUtils.getConnectionFactory(),
                                connectionFactory.getTopology_url(), null, connection, idtemp.getBroker());
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
                        connection = artemisJmsUtils.getConnection();
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

