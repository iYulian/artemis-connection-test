package org.example.artemisconnectiontest.utils;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.client.impl.TopologyMemberImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

import javax.jms.*;
import java.util.*;

/**
 * Фабрика соединений с балансировкой соединений по количеству консюмеров для заданного адреса. В любом режиме
 * распределение подключений стремится к выравниванию количества консюмеров заданного адреса
 * Распределение коннектов определяется тремя параметрами
 * compact - уменьшает количество задействованных брокеров (в комбинации с level=MC|BC)
 *           если указан address::queue, то распределение руководствуется ограничением на количество подключений к очереди
 *           если указан address (без ::queue), то учитывется общее количество всех консюмеров адреса без ограничений
 * sticky - выбирает только первое подключение, остальные повторяют выбор с учётом HA
 * level (MC|BC|BOOS) - способ "упаковки" соединений по брокерам
 *      MC - при compact=true в одной HA-паре подключения к адресу допускаются только к одному брокеру
 *           при compact=false сначала дополняются одинокие консюмеры на адресах, затем занимаются другие брокеры так,
 *           чтобы один адрес прослушивало не менее двух консюмеров
 *
 * Предусмотрены методы получения дополнительных соединений
 */
public class PlatzCard extends ActiveMQConnectionFactory {
    final HALevel level; //критичность получателя
    final boolean compact; //снижать количество (пар) брокеров, к которым выполняется подключение
    final String address;//адрес для проверки количества консюмеров
    final String _url_topology, /*только для получения топологии, сюда лучше включать брокеры чужого ЦОД*/
            _url_DC;//для создания подключений
    private volatile Topology    topology;
    //признак постоянства выбора брокера в режиме фабрики соединения - выбор только для первого вызова
    boolean sticky; public boolean getSticky (){return sticky;} public void setSticky (boolean s){sticky = s;}
    private volatile ServerLocatorImpl   leader;

    public enum HALevel{
        MC, //исключать подключение к очереди, если на парном брокере есть подключения к адресу
        BC, //подключаться преимущественно к очереди, если на парном брокере есть подключения к адресу
        BOOS//не проверять парный брокер
    }

    /**
     * базовый конструктор создаёт фабрику соединений, размазывающую коннекты по брокерам
     * с учётом ограничения по максимальному количеству подключения к очереди
     * @param url указатель на подключение к своему ЦОД
     * @param address адрес, для которого создаётся подключение, может быть null
     */
    public PlatzCard(String url, String address){
        this (url,url,address, HALevel.BOOS, false);
    }
    /**
     * Конструктор создаёт фабрику соединений, размазывающую коннекты по брокерам
     * с учётом ограничения по максимальному количеству подключения к очереди
     * @param url указатель на подключение к своему ЦОД
     * @param url_topology указатель на все брокеры кластера для получения топологии в случае упавшего своего ЦОД
     * @param address адрес, для которого создаётся подключение, может быть null
     */
    public PlatzCard(String url, String url_topology, String address){
        this (url,url_topology,address, HALevel.BOOS, false);
    }
    /**
     *  конструктор создаёт фабрику соединений, размазывающую коннекты по брокерам
     * с учётом ограничения по максимальному количеству подключения к очереди
     * @param url указатель на подключение к своему ЦОД
     * @param address адрес, для которого создаётся подключение, может быть null
     * @param level критичность подключения
     */
    public PlatzCard(String url, String address, HALevel level){
        this (url,url,address,level, false);
    }
    /**
     * конструктор создаёт фабрику соединений, размазывающую коннекты по брокерам
     * с учётом ограничения по максимальному количеству подключения к очереди
     * @param url указатель на подключение к своему ЦОД
     * @param address адрес, для которого создаётся подключение, может быть null
     * @param level критичность подключения
     * @param compact сокращать количество брокеров для подключения
     */
    public PlatzCard(String url, String address, HALevel level, boolean compact){
        this (url,url,address,level, compact);
    }
    /**
     * конструктор создаёт фабрику соединений, размазывающую коннекты по брокерам
     * с учётом ограничения по максимальному количеству подключения к очереди
     * @param url указатель на подключение к своему ЦОД
     * @param url_topology указатель на все брокеры кластера для получения топологии в случае упавшего своего ЦОД
     * @param address адрес, для которого создаётся подключение, может быть null
     * @param level критичность подключения
     * @param compact сокращать количество брокеров для подключения
     */
    public PlatzCard(String url, String url_topology, String address, HALevel level, boolean compact){
        super (url);
        _url_DC = url;
        _url_topology = (url_topology == null || url_topology.isEmpty ())?url:url_topology;
        this.level = level;
        this.compact = compact;
        this.address = address;
    }

    /**
     * Метод реализует базовый алгоритм фабрики соединений
     * @return соединение к брокеру в своём ЦОД, либо к резервному брокеру в чужом ЦОД
     * @throws JMSException
     */
    @Override
    public Connection createConnection () throws JMSException{
        try {
            Connection c = null;
            if (sticky == false || leader == null)
                c = createConsumerConnection (address, level, compact);
            else
                c = followerConnection ();
            if (c == null)
                throw new JMSException ("NOT_CONNECTED");
            if (leader == null)
                leader = (ServerLocatorImpl) ((ActiveMQConnection)c).getSessionFactory ().getServerLocator ();
            return c;
        } catch (InterruptedException e) {
            throw new JMSException ("NOT_CONNECTED");
        }
    }
    static final int backup_shift = 100;//сдвиг портов colocated_backup_01 по умолчанию.
    /**
     * Метод создаёт соединение для доступа к очереди address с учётом количества уже установленных подключений к очереди
     * если address - это адрес, то оценивается все очереди, либо оценивается конкретная очередь.
     * Для критичности HA=HALevel.BOOS оценивается только распределение консюмеров в указанных для фабрики соединений
     * статических коннекторах. При указании compact=false будут выбираться брокеры с наименьшим количеством консюмеов.
     * Брокеры, на которых адреса нет, либо нет консюмеров у адреса - в приоритете. При указании compact=true будут
     * находится брокеры, у которых уже есть консюмеры, но выбираться будут те, у которых консюмеров меньше.
     * Если адрес уже содержит максимальное количество консюмеров, такой брокер не будет выбран.
     * Если есть несколько брокеров с равным и наименьшим количеством консюмеров - будет выбран случайный из них.
     * Для критичности HA=HALevel.BС будут учитываться консюеры на парных брокерах в сумме с консюмерами на
     * рассматриваемом брокере.
     * Для критичности HA=HALevel.MС будут исключаться те брокеры, для которых на парных есть консюмеры.
     * Для HA=HALevel.MС и compact=false будут выдаваться брокеры для второго коннекта к адресу, затем остальные
     * Для HA=HALevel.MС и compact=true будут выдаваться брокеры уже с консюмерами, чтобы сократить количество брокеров
     * Метод для реализации выбора устанавливает коннекты ко всем брокерам из url фабрики соединений, а если
     * HA!=HALevel.BOOS - и к парным брокерам кластера (в другой ЦОД). После выбора закрываются все соединения.
     * Для оптимизации можно использовать сначала сбор всех коннектов заранее, но их надо закрывать самостоятельно
     *         PlatzCard cf = new PlatzCard (args[0]);
     *         Map<Connection,Connection> connectors = cf.createConnectors ();
     *         Connection connection1 = cf.getConsumerConnection (connectors, address1, HALevel.MC, true);
     *         Connection connection2 = cf.getConsumerConnection (connectors, address2, HALevel.MC, true);

     * @param address адрес/очередь
     *                если указан адрес, возвращается соединение без учёта ограничений на количество консюмеров на очереди,
     *                подразумевается, что при превышении для существующей очереди, будет создаваться дополнительная очередь.
     *                если указана очередь (address::address, address::queue), выбирается подключение с учётом ограничения
     *                максимального количества консюмеров на очереди
     * @param HA      критичность подключаемого клиента
     * @param compact выбирать брокер с максимальным/минимальным количество установленных подключений
     *                true - для подключения с повышением критичности
     *                false - для подключения с понижением критичности
     * @return подключение с возможностью создать MessageConsumer-а для указанной очереди
     */
    public Connection createConsumerConnection (String address, HALevel HA, boolean compact)
            throws InterruptedException {
        return  getConsumerConnection ( null, address, HA, compact);
    }

    /**
     * Метод выдаёт подключение к тому же брокеру с учётом HA
     * Проверяется точка первоначального подключения, если подключение перепрыгнуло при failover брокера, то выдаётся
     * новое подключение к резервному брокеру
     * @param _connection подключение-образец
     * @param timeout время ожидания соединения, если _connection не подключён к брокеру
     * @return новое соединение к тому же брокеру, что и _connection, либо null, если _connection не подключён к брокеру
     * @throws InterruptedException
     */
    public static Connection splitConnection (Connection _connection, long timeout)
            throws InterruptedException, JMSException {
        ActiveMQConnection connection = (ActiveMQConnection) _connection;

        if (connection.getSessionFactory ().isClosed ())
            return null;
        ServerLocatorImpl   locator = (ServerLocatorImpl)connection.getSessionFactory ().getServerLocator ();
        RemotingConnection  remote = connection.getSessionFactory ().getConnection ();

        long    start = System.currentTimeMillis ();

        while (remote == null && (System.currentTimeMillis () - timeout) > start){
            Thread.sleep (100);//ждём соединение
            remote = connection.getSessionFactory ().getConnection ();
        }
        if (remote == null)
            return  null;//нет соединения
        String broker = remote.getRemoteAddress ();

        TransportConfiguration clone = new TransportConfiguration (locator.getInitialConnectors ()[0].getFactoryClassName (),
                new HashMap<> (locator.getInitialConnectors ()[0].getParams ()));
        String host = broker.split ("/")[0];
        String port = broker.split (":")[1];
        clone.getParams ().put (TransportConstants.HOST_PROP_NAME,host);
        clone.getParams ().put (TransportConstants.PORT_PROP_NAME,port);
        ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, clone);
        cf2.getServerLocator ().setLocatorConfig (locator.getLocatorConfig ());

        return cf2.createConnection ();
    }

    /**
     * Метод получения повторного подключения к выбранному ранее брокеру, но с учётом HA
     * @return
     * @throws InterruptedException
     * @throws JMSException
     */
    public Connection followerConnection ()
            throws JMSException {
        if (leader == null)
            return null;
        TransportConfiguration clone = new TransportConfiguration (leader.getInitialConnectors ()[0].getFactoryClassName (),
                new HashMap<> (leader.getInitialConnectors ()[0].getParams ()));
        ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, clone);
        cf2.getServerLocator ().setLocatorConfig (leader.getLocatorConfig ());

        try {
            return cf2.createConnection();
        }
        catch (JMSException e){
            switch (((ActiveMQException)e.getCause ()).getType ()) {
                case CONNECTION_TIMEDOUT: /*ActiveMQConnectionTimedOutException*/
                case NOT_CONNECTED: /*ActiveMQNotConnectedException*/
                case DISCONNECTED: /*ActiveMQDisconnectedException*/
                case REMOTE_DISCONNECT:/*ActiveMQRemoteDisconnectException*/
                    break;//брокер недоступен, надо искать реплику
                default:
                    throw e;
            }
            if (topology == null)
                topology = anyTopology (this,_url_topology, null);
            //тут 100% должна быть топология
            TransportConfiguration backup = findBackup (clone,topology);//поиск резервной реплики
            if (backup == null)
                backup = findLive (clone,topology);
            if (backup == null)
                throw new JMSException  ("NOT_CONNECTED");
            cf2 = new ActiveMQConnectionFactory (true, backup);
            cf2.getServerLocator ().setLocatorConfig (leader.getLocatorConfig ());
            return cf2.createConnection ();//в случае недоступности реплики будет выход по JMSException
        }
    }

    /**
     * Метод используется для получения топологии из одного из доступных источников
     * @param cf фабрика соединений для своего ЦОД, используется а первую очередь
     * @param _url_topology указатель на все брокеры кластера
     * @param connection открытое соедиенение
     * @return топология, или null
     * @throws JMSException при невозможности подключиться к кластеру
     */
    public static Topology anyTopology (ActiveMQConnectionFactory cf,
                                        String   _url_topology,
                                        ActiveMQConnection connection) throws JMSException {
        Topology topology = null;
        if (((ServerLocatorImpl) cf.getServerLocator()).isReceivedTopology ())
            topology = ((ServerLocatorImpl) cf.getServerLocator()).getTopology ();
        if (topology == null && connection != null && connection.getSessionFactory ().isClosed () == false){
            topology = connection.getSessionFactory ().getServerLocator ().getTopology ();
        }
        if (topology == null){
            try {
                connection = (ActiveMQConnection) cf.createConnection ();
            }catch (JMSException ee){//нет ни одного брокера в ЦОД
                switch (((ActiveMQException)ee.getCause ()).getType ()) {
                    case CONNECTION_TIMEDOUT: /*ActiveMQConnectionTimedOutException*/
                    case NOT_CONNECTED: /*ActiveMQNotConnectedException*/
                    case DISCONNECTED: /*ActiveMQDisconnectedException*/
                    case REMOTE_DISCONNECT:/*ActiveMQRemoteDisconnectException*/
                        break;//брокер недоступен, надо искать реплику
                    default:
                        throw ee;
                }
                //если нет ни одного брокера в кластере, будет Exception с выходом из метода
                connection = (ActiveMQConnection) new ActiveMQConnectionFactory (_url_topology).createConnection ();
                topology = connection.getSessionFactory ().getServerLocator ().getTopology ();
                connection.close ();
            }
        }
        return topology;
    }
    /**
     * Метод создаёт подключение к брокеру по строке адреса подключения, полученного из предыдущей сессии вызовом
     *              connection.getSessionFactory ().getConnection ().getRemoteAddress ()
     *              адрес подключения к брокеру в этом случае выглядит так localhost/127.0.0.1:61616
     * @param broker адрес брокера в формате fqdn/IP:port, например, localhost/127.0.0.1:61616
     * @return подключение, или null, если broker соответствует _connection - возвращается _connection
     * @throws JMSException при невозможности подключиться к кластеру
     */
    public Connection brokerConnection (String broker) throws JMSException {
        return brokerConnection (this,_url_topology, topology, null, broker);
    }
    /**
     * Метод создаёт подключение к брокеру по строке адреса подключения, полученного из предыдущей сессии вызовом
     *              connection.getSessionFactory ().getConnection ().getRemoteAddress ()
     *              адрес подключения к брокеру в этом случае выглядит так localhost/127.0.0.1:61616
     * @param _connection любое подключение к кластеру, или null
     * @param broker адрес брокера в формате fqdn/IP:port, например, localhost/127.0.0.1:61616
     * @return подключение, или null, если broker соответствует _connection - возвращается _connection
     * @throws JMSException при невозможности подключиться к кластеру
     */
    public Connection brokerConnection (Connection _connection, String broker) throws JMSException {
        return brokerConnection (this,_url_topology, topology, _connection, broker);
    }
    /**
     * Метод создаёт подключение к брокеру по строке адреса подключения, полученного из предыдущей сессии вызовом
     *              connection.getSessionFactory ().getConnection ().getRemoteAddress ()
     *              адрес подключения к брокеру в этом случае выглядит так localhost/127.0.0.1:61616
     * @param _cf фабрика соединений к своему ЦОД
     * @param _url_topology указатель на все брокеры кластера
     * @param topology топология, или null
     * @param _connection любое подключение к кластеру, или null
     * @param broker адрес брокера в формате fqdn/IP:port, например, localhost/127.0.0.1:61616
     * @return подключение, или null, если broker соответствует _connection - возвращается _connection
     * @throws JMSException при невозможности подключиться к кластеру
     */
    public static Connection brokerConnection (ConnectionFactory _cf,
                                               String   _url_topology,
                                               Topology topology,
                                               Connection _connection, String broker)
            throws JMSException {
        ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory)_cf;
        ActiveMQConnection connection = (ActiveMQConnection) _connection;
        String host = broker.split ("/")[0];
        String port = broker.split (":")[1];
        //проверим, не подходит ли нам предоставленное соединение
        if (connection != null && connection.getSessionFactory ().getConnection () != null){
            if (broker.equalsIgnoreCase (connection.getSessionFactory ().getConnection ().getRemoteAddress ()))
                return connection;
        }
        //проверим есть ли требуемый брокер среди брокеров первоначального подключения в url в предоставленной фабрике
        ServerLocatorImpl   locator = (ServerLocatorImpl)cf.getServerLocator ();
        for (TransportConfiguration tr: locator.getInitialConnectors ()){//цикл по всем брокерам в фабрике
            if (host.equalsIgnoreCase ((String) tr.getParams ().get (TransportConstants.HOST_PROP_NAME))
             && port.equals (tr.getParams ().get (TransportConstants.PORT_PROP_NAME))){//проверка хоста и порта
                //брокер найден, создаём транспортную конфигурацию для подключения
                TransportConfiguration clone = new TransportConfiguration (tr.getFactoryClassName (),
                        new HashMap<> (locator.getInitialConnectors ()[0].getParams ()));
                ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, clone);
                cf2.getServerLocator ().setLocatorConfig (locator.getLocatorConfig ());

                try {//пытаемся подключиться
                    return cf2.createConnection ();
                }
                catch (JMSException e){
                    switch (((ActiveMQException)e.getCause ()).getType ()) {
                        case CONNECTION_TIMEDOUT: /*ActiveMQConnectionTimedOutException*/
                        case NOT_CONNECTED: /*ActiveMQNotConnectedException*/
                        case DISCONNECTED: /*ActiveMQDisconnectedException*/
                        case REMOTE_DISCONNECT:/*ActiveMQRemoteDisconnectException*/
                            break;//брокер недоступен, надо искать реплику
                        default:
                            throw e;
                    }
                    if (topology == null)
                        topology = anyTopology (cf,_url_topology, connection);
                    //тут 100% должна быть топология
                    clone = findBackup (tr,topology);//поиск резервной реплики
                    cf2 = new ActiveMQConnectionFactory (true, clone);
                    cf2.getServerLocator ().setLocatorConfig (locator.getLocatorConfig ());
                    return cf2.createConnection ();//в случае недоступности реплики будет выход по JMSException
                }
            }
        }
        //broker указывает на резервную реплику, надо попытаться найти основной брокер, для этого нужна топология
        if (topology == null)
            topology = anyTopology (cf,_url_topology, connection);
        if (topology == null)
            return null;//безвыходная ситуация
        TransportConfiguration clone = findLive (host, port, topology);//указатель на основной брокер
        if (clone == null)
            return null;//скорее всего указан брокер из другого ЦОД
        host = (String) clone.getParams ().get (TransportConstants.HOST_PROP_NAME);
        port = (String) clone.getParams ().get (TransportConstants.PORT_PROP_NAME);
        //проверим наличие найденного транспорта на принадлежность к своему ЦОД
        for (TransportConfiguration tr: locator.getInitialConnectors ()) {//цикл по всем брокерам в фабрике
            if (host.equalsIgnoreCase((String) tr.getParams().get(TransportConstants.HOST_PROP_NAME))
                    && port.equals(tr.getParams().get(TransportConstants.PORT_PROP_NAME))) {//проверка хоста и порта
                //надо сделать копию транспортного указателя, чтобы не испортить топологию фабрики
                clone = new TransportConfiguration (clone.getFactoryClassName (),
                        new HashMap<> (clone.getParams ()));
                ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, clone);
                cf2.getServerLocator ().setLocatorConfig (locator.getLocatorConfig ());
                //пытаемся подключиться
                return cf2.createConnection ();
            }
        }
        return null;
    }

    /**
     * Метод возвращает подключение к парному брокеру по подключению к master из своего ЦОД
     * @param t подключение к master из своего ЦОД
     * @return
     */
    public TransportConfiguration findPartner (TransportConfiguration t) {
        if (topology == null)
            return null;
        TransportConfiguration backup = null;
        Object host = t.getParams ().get (TransportConstants.HOST_PROP_NAME);
        Object port = t.getParams ().get (TransportConstants.PORT_PROP_NAME);

        for (TopologyMemberImpl member : topology.getMembers ()) {
            if (host.equals (member.getLive ().getParams ().get (TransportConstants.HOST_PROP_NAME))
                    && port.equals (member.getLive ().getParams ().get (TransportConstants.PORT_PROP_NAME))) {
                String host_backup = (String) member.getBackup ().getParams ().get (TransportConstants.HOST_PROP_NAME);
                String port_backup = (String) member.getBackup ().getParams ().get (TransportConstants.PORT_PROP_NAME);
                int backup_port = Integer.parseInt (port_backup.toString ()) - backup_shift;

                for (TopologyMemberImpl other : topology.getMembers ()) {
                    TransportConfiguration live = other.getLive ();
                    String host_live = (String) live.getParams ().get (TransportConstants.HOST_PROP_NAME);
                    String port_live = (String) live.getParams ().get (TransportConstants.PORT_PROP_NAME);

                    if (host_live.equals (host_backup) && backup_port == Integer.parseUnsignedInt (port_live.toString ())){
                        backup = live;
                        break;
                    }
                }
            }
        }
        return backup;
    }

    /**
     * Метод выдаёт подключение к бекапу по подключению к мастеру
     * Топология должна быть загружена
     * @param t подключение к master
     * @return транспорт к бэкапу
     */
    public  TransportConfiguration findBackup (TransportConfiguration t) {
        return findBackup (t,topology);
    }
    public  static TransportConfiguration findBackup (TransportConfiguration t, Topology topology) {
        if (topology == null)
            return null;
        Object host = t.getParams ().get (TransportConstants.HOST_PROP_NAME);
        Object port = t.getParams ().get (TransportConstants.PORT_PROP_NAME);

        for (TopologyMemberImpl member : topology.getMembers ()) {
            if (host.equals (member.getLive ().getParams ().get (TransportConstants.HOST_PROP_NAME))
                    && port.equals (member.getLive ().getParams ().get (TransportConstants.PORT_PROP_NAME)))
                return member.getBackup ();
        }
        return null;
    }

    /**
     * Находит в топологии основной по резервному брокеру
     * @param backup транспорт к резервному брокеру
     * @param topology топология
     * @return
     */
    public  static TransportConfiguration findLive (TransportConfiguration backup, Topology topology) {
        if (topology == null)
            return null;
        String host = (String) backup.getParams ().get (TransportConstants.HOST_PROP_NAME);
        String port = (String) backup.getParams ().get (TransportConstants.PORT_PROP_NAME);
        return findLive (host, port, topology);
    }
    public  static TransportConfiguration findLive (String host, String port, Topology topology) {
        for (TopologyMemberImpl member : topology.getMembers ()) {
            if (host.equals (member.getBackup ().getParams ().get (TransportConstants.HOST_PROP_NAME))
                    && port.equals (member.getBackup ().getParams ().get (TransportConstants.PORT_PROP_NAME)))
                return member.getLive ();
        }
        return null;
    }
    /**
     * Метод даёт карту master-master <заменитель,партнёр> для брокеров. где первый master является активизированным
     * backup (заменитель) для всех недоступных брокеров из @near (они должны быть перечислены в @empty)
     * @param empty - перечень недоступных подключений к брокерам из "своего" ЦОД
     * @return карта <заменитель,партнёр>
     */
    public Map<TransportConfiguration,TransportConfiguration> findActiveBackups (Set<TransportConfiguration> empty) {
        return findActiveBackups (getStaticConnectors (), empty, topology);
    }
    /**
     * Метод даёт карту master-master <заменитель,партнёр> для брокеров. где первый master является активизированным
     * backup (заменитель) для всех недоступных брокеров из @near (они должны быть перечислены в @empty)
     * @param near перечень подключений для "своего ЦОД", должны быть все брокеры из своего ЦОД
     * @param empty - перечень недоступных подключений к брокерам из "своего" ЦОД
     * @param topology - топология кластера
     * @return карта <заменитель,партнёр>
     */
    public static Map<TransportConfiguration,TransportConfiguration> findActiveBackups (TransportConfiguration near[],
                                                                 Set<TransportConfiguration> empty, Topology topology) {
        if (topology == null)
            return null;
        Map<TransportConfiguration,TransportConfiguration> active = null;
        Iterator<TransportConfiguration> fetcher = empty.iterator ();

        for (TopologyMemberImpl member : topology.getMembers ()) {
            if (fetcher.hasNext () == false)
                break;
            if (member.getBackup () != null)
                continue;

            String member_host = (String) member.getLive ().getParams ().get (TransportConstants.HOST_PROP_NAME);
            String member_port_string = (String)member.getLive ().getParams ().get (TransportConstants.PORT_PROP_NAME);
            int member_port = Integer.parseUnsignedInt (member_port_string);

            boolean bingo = false;
            for (TransportConfiguration link: near){
                String link_port_string = link.getParams ().get (TransportConstants.PORT_PROP_NAME).toString ();

                if (member_host.equalsIgnoreCase (link.getParams ().get (TransportConstants.HOST_PROP_NAME).toString ())
                 && (member_port_string.equals (link_port_string)
                  || member_port == Integer.parseUnsignedInt (link_port_string) + backup_shift))
                    continue;
                bingo = true;
                break;
            }
            if (bingo == false)
                continue;
            TransportConfiguration partner = null;
            bingo = false;

            for (TopologyMemberImpl challenger : topology.getMembers ()){
                if (member_host.equalsIgnoreCase ((String) challenger.getLive ().getParams ().get (TransportConstants.HOST_PROP_NAME)) == false)
                    continue;
                String challenger_port_string = challenger.getLive ().getParams ().get (TransportConstants.PORT_PROP_NAME).toString ();

                if (member_port_string.equals (challenger_port_string))
                    continue;
                if (member_port == Integer.parseUnsignedInt (challenger_port_string) + backup_shift) {
                    bingo = true;
                    partner = challenger.getLive ();
                    break;
                }
            }
            if (bingo == false)
                continue;
            if (active == null)
                active = new HashMap<> (empty.size ());
            TransportConfiguration clone = fetcher.next ();
            Map<String,Object> params = new HashMap<> ();
            params.putAll (clone.getParams ());
            clone = new TransportConfiguration (clone.getFactoryClassName (), params);
            clone.getParams ().put (TransportConstants.HOST_PROP_NAME,member.getLive ().getParams ().get (TransportConstants.HOST_PROP_NAME));
            clone.getParams ().put (TransportConstants.PORT_PROP_NAME,member.getLive ().getParams ().get (TransportConstants.PORT_PROP_NAME));
            TransportConfiguration stanIn = null;

            if (partner != null) {
                params = new HashMap<> ();
                params.putAll (clone.getParams ());
                stanIn = new TransportConfiguration (clone.getFactoryClassName (), params);
                stanIn.getParams ().put (TransportConstants.HOST_PROP_NAME, partner.getParams ().get (TransportConstants.HOST_PROP_NAME));
                stanIn.getParams ().put (TransportConstants.PORT_PROP_NAME, partner.getParams ().get (TransportConstants.PORT_PROP_NAME));
            }
            active.put (clone,stanIn);
        }
        return active;
    }

    void preloadTopology (){
        if (topology == null){
            try {
                ActiveMQConnection c = (ActiveMQConnection) new ActiveMQConnectionFactory (_url_topology).createConnection ();
                topology = c.getSessionFactory ().getServerLocator ().getTopology ();
                c.close ();
            } catch (JMSException e) {//ни одного брокера нет, либо с сертификатами непорядок
            }
        }
    }

    /**
     * Метод загружает соединения к доступным брокерам своего ЦОД, к каждому брокеру его парный в карте
     * @return карта парных брокеров master-master, ключ - брокер свого ЦОД
     */
    public Map<Connection,Connection> createConnectors () {
        TransportConfiguration transport[] = getStaticConnectors ();
        Map<Connection,Connection> connectors = new HashMap<> (transport.length * 2);
        Set<TransportConfiguration> chance = new HashSet<>();

        for (TransportConfiguration t: transport) {

            ActiveMQConnection connection, replica = null;
            TransportConfiguration backup = null;

            try {
                ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, t);
                cf2.getServerLocator ().setLocatorConfig (getServerLocator ().getLocatorConfig ());
                connection = (ActiveMQConnection) cf2.createConnection ();
            } catch (JMSException be){
                switch (((ActiveMQException)be.getCause ()).getType ()) {
                    case CONNECTION_TIMEDOUT: /*ActiveMQConnectionTimedOutException*/
                    case NOT_CONNECTED: /*ActiveMQNotConnectedException*/
                    case DISCONNECTED: /*ActiveMQDisconnectedException*/
                    case REMOTE_DISCONNECT:/*ActiveMQRemoteDisconnectException*/
                        chance.add (t);//брокер недоступен, надо искать реплику
                    default:
                        continue;
                }
            }

            if (topology == null)
                topology = connection.getSessionFactory ().getServerLocator ().getTopology ();

            if ((backup = findPartner (t)) != null) try {
                ActiveMQConnectionFactory bf = new ActiveMQConnectionFactory (false, backup);
                replica = (ActiveMQConnection) bf.createConnection ();
            } catch (Exception e) {
            }
            connectors.put (connection, replica);
        }

        if (chance.isEmpty () == false)
            preloadTopology ();

        if (topology != null && chance.isEmpty () == false) {
            Map <TransportConfiguration,TransportConfiguration> failover = findActiveBackups (transport, chance, topology);
            if (failover != null) failover.forEach ((master, slaver) -> {
                ActiveMQConnection master_connection = null, slaver_connection = null;

                try {
                    ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, master);
                    cf2.getServerLocator ().setLocatorConfig (getServerLocator ().getLocatorConfig ());
                    master_connection = (ActiveMQConnection) cf2.createConnection ();
                    if (slaver != null)
                        slaver_connection = (ActiveMQConnection) new ActiveMQConnectionFactory (false, slaver).createConnection ();
                } catch (JMSException be) {
                }
                if (master_connection != null)
                    connectors.put (master_connection, slaver_connection);
            });
        }
        return connectors;
    }

    /**
     * Метод выбирает из уже установленных соединений одно, удовлетворяющее требованиям "расстановки" соединений
     * в соответствие с критичностью клиента и кластера
     * @param connectors карта master-master пар брокеров, ключ - брокер своего ЦОД, либо null
     * @param address адрес для проверки количества консюмеров
     * @param HA критичность системы
     * @param compact - true, если надо упаковывать соединения (снижать количество брокеров), игнорируется для HA=BOOS
     * @return соединение, либо null. Если предоставлена карта connectors - возвращается один из ключей.
     *          Если connectors=null при вызове, результат - новое соединение (надо закрывать)
     * @throws InterruptedException испольуется случайная задержка в пределах 1с для снжения конкуренции
     */
    public Connection getConsumerConnection (Map<Connection,Connection> connectors,
                                                    String address, HALevel HA, boolean compact)
            throws InterruptedException {
        TransportConfiguration transport[] = getStaticConnectors ();
        Map<ActiveMQConnection, Integer> connections = new HashMap<> ();
        String address_queue[];
        boolean free_connections = connectors == null;
        SimpleString a = null, q = null;
        boolean def = false;//true - наличие :: в адресе
        Set<TransportConfiguration> chance = new HashSet<>();

        //торопится не надо, иначе одновременные действия будут приводить к неравномерности
        Thread.sleep (new Random ().nextInt (1000));

        if (address != null && address.isEmpty () == false) {
            if ((address_queue = address.split ("::")) != null && address_queue.length > 0) {
                a = new SimpleString (address_queue[0]);//адрес
                q = (def = (address_queue.length > 1)) ? new SimpleString (address_queue[1]) : a;//очередь
            } else
                return null;
        }
        else
            compact = false;

        if (free_connections) {
            for (TransportConfiguration t : transport) {
                //Object host = t.getParams().get(TransportConstants.HOST_PROP_NAME);
                //Object port = t.getParams().get(TransportConstants.PORT_PROP_NAME);
                ActiveMQConnection connection;
                ActiveMQConnection backup = null;

                try {
                    ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, t);
                    cf2.getServerLocator ().setLocatorConfig (getServerLocator ().getLocatorConfig ());
                    connection = (ActiveMQConnection) cf2.createConnection ();
                    connections.put (connection, null);
                } catch (Exception e) {
                    switch (((ActiveMQException)e.getCause ()).getType ()) {
                        case CONNECTION_TIMEDOUT: /*ActiveMQConnectionTimedOutException*/
                        case NOT_CONNECTED: /*ActiveMQNotConnectedException*/
                        case DISCONNECTED: /*ActiveMQDisconnectedException*/
                        case REMOTE_DISCONNECT:/*ActiveMQRemoteDisconnectException*/
                            chance.add (t);//брокер недоступен, надо искать реплику
                        default:
                            continue;
                    }
                }
                if (HA != HALevel.BOOS && compact) {
                    //надо собирать топологию и резервные подключения
                    if (topology == null)
                        topology = connection.getSessionFactory ().getServerLocator ().getTopology ();

                    if (connectors == null)
                        connectors = new HashMap<>(transport.length * 2);

                    TransportConfiguration live = findPartner (t);

                    if (live != null) try {
                        backup = (ActiveMQConnection) new ActiveMQConnectionFactory (false, live).createConnection ();
                    } catch (Exception e) {
                    }
                    connectors.put (connection, backup);
                }
            }
            if (chance.isEmpty () == false) {
                if (topology == null){
                    if (connections.isEmpty () == false)
                        topology = ((ActiveMQConnection)connections.keySet ().toArray ()[0]).getSessionFactory ()
                                .getServerLocator ().getTopology ();
                    else
                        preloadTopology ();
                }
                Map <TransportConfiguration,TransportConfiguration> failover = findActiveBackups (transport, chance, topology);
                if (failover != null) {
                    for (Map.Entry<TransportConfiguration, TransportConfiguration> entry : failover.entrySet ()) {
                        TransportConfiguration master = entry.getKey();
                        TransportConfiguration slaver = entry.getValue();
                        ActiveMQConnection master_connection = null, slaver_connection = null;

                        try {
                            ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory (true, master);
                            cf2.getServerLocator ().setLocatorConfig (getServerLocator ().getLocatorConfig ());
                            master_connection = (ActiveMQConnection) cf2.createConnection ();

                            if (slaver != null && HA != HALevel.BOOS && compact)
                                slaver_connection = (ActiveMQConnection) new ActiveMQConnectionFactory (false, slaver).createConnection ();
                        } catch (JMSException be) {
                        }
                        if (master_connection != null) {
                            connections.put (master_connection, null);
                            if (slaver_connection != null) {
                                if (connectors == null)
                                    connectors = new HashMap<> (failover.size ());
                                connectors.put (master_connection, slaver_connection);
                            }
                        }
                    }
                }
            }
        }
        else
            for (Connection c: connectors.keySet ())
                connections.put ((ActiveMQConnection)c,null);
        if (address != null && address.isEmpty () == false) {
            for (Map.Entry<ActiveMQConnection, Integer> link : connections.entrySet ()) {
                try {
                    ActiveMQConnection connection = link.getKey ();
                    ClientSession session = connection.getSessionFactory ().createSession (false, false);
                    ClientSession.QueueQuery queue = null;
                    ClientSession.AddressQuery addressQuery = session.addressQuery (a);
                    int limit = def ? Integer.MAX_VALUE : 0;
                    boolean b_consumers = false;

                    if (HA != HALevel.BOOS && compact) {
                        ActiveMQConnection b_connection = (ActiveMQConnection) connectors.get (connection);

                        if (b_connection != null) try {
                            ClientSession b_session = b_connection.getSessionFactory ().createSession (false, false);
                            ClientSession.AddressQuery b_addressQuery = b_session.addressQuery (a);

                            for (SimpleString s : b_addressQuery.getQueueNames ()) {
                                ClientSession.QueueQuery b_queue = b_session.queueQuery (s);

                                if (b_queue != null && b_queue.isExists ()
                                        && (b_consumers = (b_queue.getConsumerCount () > 0)))
                                    break;
                            }
                            b_session.close();
                        } catch (Exception e) {
                            //System.out.println (e);
                        }
                    }

                    if (addressQuery != null && addressQuery.isExists ()) {
                        int consumers = 0;
                        //addressQuery.isAutoCreateQueues ();
                        if (addressQuery.getDefaultMaxConsumers () > 0 && def)
                            limit = addressQuery.getDefaultMaxConsumers ();
                        for (SimpleString s : addressQuery.getQueueNames ()) {
                            if (def) {
                                if (s.equals (q)) {
                                    if ((queue = session.queueQuery (s)) != null && queue.isExists ()) {
                                        if (queue.getMaxConsumers() > 0)
                                            limit = queue.getMaxConsumers ();
                                        limit -= queue.getConsumerCount ();
                                        consumers += queue.getConsumerCount ();
                                        //queue.getMessageCount ();
                                    }
                                    break;
                                }
                            } else {
                                if ((queue = session.queueQuery (s)) != null && queue.isExists ()) {
                                    limit += queue.getConsumerCount ();
                                    consumers += queue.getConsumerCount ();
                                }
                            }
                        }
                        if (HA == HALevel.MC && consumers == 1 && def)
                            limit += 2;
                    }
                    if (b_consumers) {//только в режиме compact для MC и BC
                        if (HA == HALevel.MC) {
                            limit = def ? 0 : -1;
                        } else/* if (HA == HALevel.BC) */ {
                            if (def) {
                                if (limit > 0) {
                                    if (limit == Integer.MAX_VALUE)
                                        limit = Integer.MAX_VALUE / 2;
                                }
                            } else
                                limit++;//fixme надо придумать, как ранжировать адреса без очередей
                        }
                    }
                    link.setValue(limit);
                    session.close();
                } catch (Exception e) {
                }
            }
        }
        ActiveMQConnection connection = null;
        ArrayList<ActiveMQConnection> order = new ArrayList<> (transport.length);

        if (compact == def){
            int     limit = Integer.MAX_VALUE;
            for (Map.Entry<ActiveMQConnection, Integer> link: connections.entrySet ()){
                int value = (link.getValue () != null)?link.getValue ():0;
                ActiveMQConnection key = link.getKey ();
                if ((value > 0 || def == false) && limit >= value){
                    if (limit != value)
                        order.clear ();
                    order.add (key);
                    connection = key;
                    limit = value;
                }
            }
        }
        else{
            int     limit = 0;
            for (Map.Entry<ActiveMQConnection, Integer> link: connections.entrySet ()){
                if ((link.getValue () > 0 || def == false) && limit <= link.getValue ()){
                    if (limit != link.getValue ())
                        order.clear ();
                    order.add (link.getKey ());
                    connection = link.getKey ();
                    limit = link.getValue ();
                }
            }
        }
        if (order.size () > 1){
            connection = order.get (new Random ().nextInt (order.size ()));
        }
        if (free_connections) {
            for (ActiveMQConnection c : connections.keySet ())
                if (c.equals(connection) == false) {
                    try {
                        c.close();
                    } catch (JMSException e) {
                    }
                }
            if (connectors != null) {
                for (Connection link: connectors.values ()){
                    try {
                        link.close ();
                    } catch (JMSException e) {
                    }
                }
            }
        }
        return connection;
    }

    /**
     * Метод для демонстрации работы фабрики
     * @param args
     * @throws InterruptedException
     * @throws JMSException
     */
    public static void main (String[] args) throws InterruptedException, JMSException, ActiveMQException {
        if (args.length < 2){
            System.out.println ("Usage: " + System.getProperty ("sun.java.command").split (" ")[0]
                    + "<url-local-DC> <address::queue|address> [count [MC|BC|other [true|false [url-remote-DC]]]]");
        }
        PlatzCard cf = new PlatzCard (args[0],
                (args.length>5)?args[5]:null,
                args[1].equalsIgnoreCase ("null")?null:args[1],
                (args.length>3)?(args[3].equalsIgnoreCase ("MC")? HALevel.MC
                        :(args[3].equalsIgnoreCase ("BC")? HALevel.BC: HALevel.BOOS)): HALevel.BOOS,
                (args.length>4)?(args[4].equalsIgnoreCase ("true")):false
                );
        //Map<Connection,Connection> connectors = createConnectors (cf);
        Set<Connection> connections = new HashSet<> (12);
        Set<Session> sessions = new HashSet<> (12);
        Set<MessageConsumer> consumers = new HashSet<> (12);
        int count = (args.length>2)?Integer.parseUnsignedInt (args[2]):1;

        for (int i = 0; i < count; i++) {
            ActiveMQConnection c = (ActiveMQConnection) cf.createConnection ();
            if (c == null)
                continue;
            //splitConnection (c,1000);
            //brokerConnection (cf, args[0], null, c, "localhost/127.0.0.1:61616");
            connections.add (c);
            Session s = c.createSession ();
            sessions.add (s);
            MessageConsumer l = s.createConsumer (s.createQueue (args[1]));
            consumers.add (l);
            System.out.println (c.getSessionFactory ().getConnection ().getRemoteAddress ());
        }
        System.out.println ("Go!");
        Thread.sleep (600000);
        consumers.forEach (l->{
            try {
                l.close();
            } catch (JMSException e) {
            }
        });
        sessions.forEach (l->{
            try {
                l.close();
            } catch (JMSException e) {
            }
        });
        connections.forEach (l->{
            try {
                l.close();
            } catch (JMSException e) {
            }
        });
    }
}
