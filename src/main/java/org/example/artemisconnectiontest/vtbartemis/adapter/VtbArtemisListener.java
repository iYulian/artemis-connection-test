package org.example.artemisconnectiontest.vtbartemis.adapter;

import org.example.artemisconnectiontest.utils.ReportFields;
import org.example.artemisconnectiontest.utils.RequestID;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
public class VtbArtemisListener {
    @JmsListener(destination = "DC.fbti-dev.FTI.FTI_TIM.SEARCH_CLIENT.REQ.Q", //запрос
            containerFactory = "ServiceFactory", concurrency = "1-1")
    public void service1 (Message request, Session session) throws JMSException, InterruptedException, IOException {
        RequestID id = null;
        TextMessage response;
        File temp = null;

        try {
            id = RequestID.from (request,session);
            //ВТБ-Артемис передаёт запросившую ТУЗ, что можно использовать для прикладной авторизации;
            //получим "долговременный" идентификатор запроса

            if (id == null) {
                System.out.println("Слушатель запросов -> id запроса NULL");
                return;//это неправильный запрос, его не надо обрабатывать
            }

            System.out.println ("Слушатель запросов -> id запроса " + id.getid());
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
            //формируем "ответ" и записываем его в файл для нескольких итераций отправки ответа
            FileOutputStream o = new FileOutputStream (tempR);
            o.write (("Service response " + id.getid ()).getBytes (StandardCharsets.UTF_8));
            o.close ();
        }

        //обработка закончена, запишем результат обработки в файл
        //пытаемся отправить ответ, после долгого перерыва это может не получиться
        //важно отправить ответ в той же сессии, что получен запрос
        session.createProducer (id.getDestination ())
                .send (id.prepareResponse ((response = session.createTextMessage ("Service response " + id.getid()))),
                        DeliveryMode.PERSISTENT, 4, 60000);

        System.out.println ("Слушатель запросов -> текст запроса: " + response.getText());

        //если отправка не удастся. что бывает после долгого перерыва,
        // можно повторить отправку из файла после обработки следующего запроса.
        //имеет смысл отправлять все сохранённые файлы, кроме
        temp.delete ();
        tempR.delete ();
    }

    //метод получает подтверждение о доставке сообщения (отчёты)
    @JmsListener(destination = "DC.fbti-dev.FTI_TIM.IGTCLOSENOTIF.REQ.Q", containerFactory = "ServiceFactory")
    public void report (Message m, Session s) throws JMSException {
        ReportFields rf = ReportFields.from (m);
        System.out.println ("Сообщение о доставке в сервис для :" + rf.getReport());

        if (s.getAcknowledgeMode () != Session.AUTO_ACKNOWLEDGE || s.getAcknowledgeMode () != Session.SESSION_TRANSACTED)
            m.acknowledge ();//Требуется для сессий INDIVIDUAL_ACKNOWLEDGE и CLIENT_ACKNOWLEDGE

    }
}
