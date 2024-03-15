package org.example.artemisconnectiontest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jms.JmsException;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsBootstrapConfiguration;

import javax.jms.JMSException;

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
@ComponentScan
@Configuration
@Import(value= JmsBootstrapConfiguration.class)
//@Lazy
public class JMSRequestReply /*implements JmsListenerConfigurer */{

    //метод эмулирует сервис: получает запрос и формирует ответ
	//конкуренцию большой ставить нет смысла, так как листнер забирает 1мБ сообщений из очереди, 
	//как правило, в очереди больше нет, поэтому остальные экземпляры листнеров будут простаивать
	//если нужна конкуренция, надо уменьшать окно косюмера consumerWindowSize в URL

    public static void main (String[] args) throws JmsException, InterruptedException, JMSException {
        // Launch the application
        SpringApplication.run (JMSRequestReply.class, args);
    }
}
