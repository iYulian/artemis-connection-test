package org.example.artemisconnectiontest.vtbartemis.adapter;

import org.example.artemisconnectiontest.utils.ReportFields;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;


public class VtbArtemisReceiver {

    public void receiveMassage(MessageConsumer consumer, ReportFields rt) throws JMSException {
        //ждём ответ, делаем полезные вещи
        do {//ждать надо в цикле, использовать селектор по корреляционному идентификатору не годится, так как
            //могут прийти запоздалые ответы от долгоиграющего сервиса
            TextMessage response = (TextMessage) consumer.receive(60000);
            //поставим небольшой таймаут, чтобы не ждать вечно в случае ошибки сервиса
            if (response == null) {
                System.out.println ("Ответ от клиента : нет ответа повтори\n\n");
                break;//новый запрос протолкнёт старые ответы
            }
            response.acknowledge ();
            //определяем, нужен ли нам этот запрос
            if (rt.isResponseSuitable (response)) {
                System.out.println ("Ответ от клиента : " + response.getText() + "\n\n");
                break;
            }
            System.out.println ("Client-> Belated response, ignore: " + response);
        }
        //и так долго...
        while (true);
    }
}
