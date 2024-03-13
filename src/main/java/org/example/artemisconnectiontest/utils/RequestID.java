package org.example.artemisconnectiontest.utils;

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.client.impl.ClientSessionImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * СОхраняемый идентификатор запроса, может быть использован для формирования ответа
 * получить можно из запроса
 * Message request = producer.receive ();
 * RequestID f = RequestID.from (request);
 * Но лучше с использованием сессии, тогда поле f.broker будет содержать адрес брокера, а не его идентификатор
 * RequestID f = RequestID.from (request,session);
 *
 * Дальше можно сохранить в качестве строки в файл, чтобы не потерять при перезагрузке
 *
 * File temp = File.createTempFile (f.getid (), null, null);
 * new FileOutputStream (temp).write (f.toString ().getBytes (StandardCharsets.UTF_8));
 *
 * После длительнйо обработки, после перезагрузки...
 * RequestID f = RequestID.from (new String (new FileInputStream (filename).readAllBytes ()),StandardCharsets.UTF_8);
 */
public class RequestID implements Serializable {
    String  id;
    String  queue;
    long    expiration;
    String  user;//заполняется плагином, для обычного запроса - null
    //указатель на брокер, либо идентификатор брокера, заполненый плагином
    String  broker;//чтобы получить указатель, надо вызывать RequestID.from (request,session);
    String  correlation;
    //fixme нет поддержки временных очередей
    static final Pattern pattern = Pattern.compile ("RequestID\\{id=ID:[a-z0-9\\-:]+,queue=queue://[\\w\\.\\-:/]+," +
            "expiration=[0-9]+,user=[\\w\\.\\-]*,broker=[\\w\\.\\-:/]*,correlation=\"[\\w\\-\\.,\\s\\=:/]*\"\\}");
    static final Pattern id_pattern = Pattern.compile ("id=ID:[a-z0-9\\-:]+,");
    //fixme нет поддержки временных очередей
    static final Pattern queue_pattern = Pattern.compile ("queue=queue://[\\w\\.\\-:/]+,");
    static final Pattern expiry_pattern = Pattern.compile ("expiration=[0-9]+,");
    static final Pattern user_pattern = Pattern.compile ("user=[\\w\\.\\-]*,");
    static final Pattern broker_pattern = Pattern.compile ("broker=[\\w\\.\\-:/]*,");
    static final Pattern corr_pattern = Pattern.compile ("correlation=\"[\\w\\-\\.,\\s\\+\\=:/]*\"");

    RequestID(String id, String queue, long expiry, String user, String broker, String correlation){
        this.id = id;
        this.queue = queue;
        this.expiration = expiry;
        this.user = user;
        this.broker = broker;
        this.correlation = correlation;
    }
    public final String getid (){return id.substring ("ID:".length ());}
    public final String getMessageId (){return /*"ID:"+*/id;}
    public final String getqueue (){return  queue;}
    //fixme нет поддержки временных очередей
    public final String getQueue (){return  queue.substring ("queue://".length ());}
    public final long getExpiration (){return  expiration;}
    public final String getCorrelation (){return  correlation;}
    public final String getUser (){return  correlation;}
    public final String getBroker (){return  broker;}

    public String getCorrelationID (){
        return "" + ((correlation == null || correlation.isEmpty ())?getMessageId ():correlation);
    }
    public String getCorrelationString (){
        return "\'" + getCorrelationID () + "\'";
    }

    public String getSelector (){
        return "JMSCorrelationID=\'" + getCorrelationID () + "\'";
    }

    public static String getSelector (RequestID ... ids ){
        StringBuffer    result = new StringBuffer(ids.length*65);
        for (RequestID id: ids ){
            if (result.length() > 0)
                result.append (" OR ");
            result.append (id.getSelector());
        }
        return result.toString ();
    }

    public static String getSelector (Set<RequestID> c){
        return RequestID.getSelector (c.toArray (RequestID[]::new));
    }

    public final Destination getDestination (){
        return ActiveMQDestination.createDestination (getQueue (), ActiveMQDestination.TYPE.QUEUE);
    }

    public static RequestID from (String s){
        return parse (s);
    }
    public static RequestID from (Message request) throws JMSException {
        return from (request,null);
    }

    public static RequestID from (Message request, Session session) throws JMSException {
        String  id = request.getJMSMessageID ();
        String  queue = request.getStringProperty ("JMSReplyTo");
        long    expiry  = request.getJMSExpiration ();
        String user = request.getStringProperty ("VTBUser");
        String broker = (session == null)?request.getStringProperty ("VTBBroker")
                :((ClientSessionImpl)((ActiveMQSession)session).getCoreSession()).getConnection().getRemoteAddress ();
        String correlation = request.getJMSCorrelationID ();

        if (id == null || id.isEmpty ())
            return null;

        if (id.startsWith ("ID:") == false)
            return null;

        //fixme нет поддержки временных очередей
        if (queue == null || queue.isEmpty () || queue.startsWith ("queue://") == false)
            return null;

        if (queue.contains("::") == false)
            broker = null;

        return new RequestID (id/*.substring ("ID:".length ())*/,queue, expiry, user, broker, correlation);
    }
    public static RequestID from (File f) throws IOException {
        FileInputStream in = new FileInputStream (f);
        RequestID id;
        id = from (in);
        in.close ();
        return id;
    }
    public static RequestID from (InputStream in) throws IOException {
        return parse (new String (in.readAllBytes (),StandardCharsets.UTF_8));
    }
    public void to (File f) throws IOException {
        FileOutputStream o = new FileOutputStream (f);
        to (o);
        o.close ();
    }
    public void to (OutputStream o) throws IOException {
        o.write (toString ().getBytes (StandardCharsets.UTF_8));
    }

    public Message prepareResponse (Message response) throws JMSException {
        if (correlation == null || correlation.isEmpty ()) {
            response.setJMSCorrelationID (id);
        }
        else
            response.setJMSCorrelationID (correlation);
        return response;
    }
    public ClientMessage prepareResponse (ClientMessage response) throws JMSException {
        response.setAddress (getQueue ());

        if (correlation == null || correlation.isEmpty ()) {
            response.setCorrelationID (id);
        }
        else
            response.setCorrelationID (correlation);
        return response;
    }
    public boolean isResponseSuitable (Message response) throws JMSException {
        return correlation == null || (response.propertyExists ("JMSCorrelationID")
                && response.getObjectProperty ("JMSCorrelationID").equals (correlation));
    }

    @Override
    public final String toString() {
        return "RequestID{id=" + id /*ID:*/
                + ",queue=" + queue
                + ",expiration=" + expiration
                + ",user=" + ((user!=null)?user:"")
                + ",broker=" + ((broker!=null)?broker:"")
                + ",correlation=\"" + ((correlation!=null)?correlation:"") + "\"}";
    }

    public static RequestID parse (String s){
        String id;
        String queue;
        String user;
        long   expiration = 0;
        String broker;
        String correlation;
        String  name_value;
        Matcher m = corr_pattern.matcher (s);

        if (m.find () == false)
            return null;

        m = id_pattern.matcher (s);

        if (m.find ()) {
            name_value = m.group ();
            id = name_value.substring ("id=".length(),name_value.length()-1);/*ID:*/
            if (id == null || id.isEmpty ())
                return null;
        }
        else
            return null;

        m = queue_pattern.matcher (s);

        if (m.find ()) {
            name_value = m.group ();
            queue = name_value.substring ("queue=".length(),name_value.length()-1);
            if (queue == null || queue.isEmpty ())
                return null;
        }
        else
            return null;

        m = expiry_pattern.matcher (s);

        if (m.find ()) {
            String temp;
            name_value = m.group ();
            temp = name_value.substring ("expiration=".length(),name_value.length()-1);
            if (temp != null && temp.isEmpty () == false) {
                expiration = Long.parseUnsignedLong (temp);
            }
        }

        m = user_pattern.matcher (s);

        if (m.find ()) {
            name_value = m.group ();
            user = name_value.substring ("user=".length(),name_value.length()-1);
            if (user != null && user.isEmpty ())
                user = null;
        }
        else
            return null;

        m = broker_pattern.matcher (s);

        if (m.find ()) {
            name_value = m.group ();
            broker = name_value.substring ("broker=".length(),name_value.length()-1);
            if (broker != null && broker.isEmpty ())
                broker = null;
        }
        else
            return null;

        m = corr_pattern.matcher (s);
        if (m.find ()) {
            name_value = m.group ();
            correlation = name_value.substring ("correlation=\"".length(),name_value.length()-1);
            if (correlation != null && correlation.isEmpty ())
                correlation = null;
        }
        else
            return null;

        return new RequestID (id, queue, expiration, user, broker, correlation);
    }

    public static void main(final String[] args){
        //fixme нет поддержки временных очередей
        RequestID f = parse ("ResponseFields{id=ID:16a2bbde-d01f-11ec-b40e-f8e43b68135b,queue=queue://Client1.Response,expiration=0,user=,broker=localhost/127.0.0.1:61617,correlation=\"any tex.,, -\"}");
        f = parse ("RequestID{id=ID:e034cd67-d9d9-11ec-9f5b-f8e43b68135b,queue=queue://DC.Client2.Reply,expiration=1653229133433,user=,broker=,correlation=\"ID:dfe65f41-d9d9-11ec-9f5b-f8e43b68135b\"}");
        //File temp = File.createTempFile (f.id, null, null);
        //new FileOutputStream (temp).write (f.toString().getBytes (StandardCharsets.UTF_8));
        //new String (new FileInputStream (f.id).readAllBytes (),StandardCharsets.UTF_8);
    }
}
