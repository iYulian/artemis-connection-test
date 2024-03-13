package org.example.artemisconnectiontest.utils;

import org.apache.activemq.artemis.jms.client.ActiveMQDestination;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.Serializable;

public class ReportFields implements Serializable {
    String VTBAddress, VTBReport, JMSReplyTo;
    Object JMSCorrelationID;
    String VTBBroker, originalId, originalAddress, originalQueue;
    long    originalExpiryTime, originalTimestamp;

    boolean delivered = true,expired,killed,replaced,acknowledged=true,forwarded, intercepted;
    boolean referer;

    public ReportFields(String q)  throws JMSException {
        if (setReplyTo (q) == false)
            throw new JMSException ("JMSReplyTo is empty");
    }
    public ReportFields(String q, boolean fail) throws JMSException {
        this (q);
        setFailReports (fail);
    }
    public ReportFields(String q, Object id) throws JMSException {
        this (q);
        setCorrelationID (id);
    }
    public ReportFields(String q, Object id, boolean fail) throws JMSException {
        this (q, id);
        setFailReports (fail);
    }
    ReportFields() throws JMSException {
        delivered = acknowledged = false;
    }

    public void setReportAddress (String q){
        if (q == null || q.isEmpty ())
            return;
        if (q.startsWith ("queue://") == false)
            VTBAddress = "queue://" + q;
        else
            VTBAddress = q;
    }
    public ReportFields putReportAddress (String s){setReportAddress (s); return this;}

    public void setReferer (boolean status){
        this.referer = status;
    }

    public boolean getDelivered () { return delivered;}
    public boolean getExpired () { return expired;}
    public boolean getKilled () { return killed;}
    public boolean getReplaced () { return replaced;}
    public boolean getAcknowledged () { return acknowledged;}
    public boolean getForwarded () { return forwarded;}
    public boolean getIntercepted () { return intercepted;}

    public void setReportTypes (boolean delivered,
                                boolean expired,boolean killed,boolean replaced,
                                boolean acknowledged,
                                boolean forwarded,
                                boolean intercepted){
        VTBReport = null;
        this.delivered = delivered ;
        this.expired = expired;
        this.killed = killed;
        this.replaced = replaced;
        this.acknowledged = acknowledged;
        this.forwarded = forwarded ;
        this.intercepted = intercepted;
    }
    public void setDelivered (boolean status){
        VTBReport = null;
        this.delivered = status;
    }
    public void setAcknowledged (boolean status){
        VTBReport = null;
        this.acknowledged = status;
    }

    /**
     * Включает отчёты об успешной доставке
     * @param status
     */
    public void setSuccessReports (boolean status){
        VTBReport = null;
        this.delivered = status;
        this.acknowledged = status;
    }

    public void setFailReports (boolean status){
        VTBReport = null;
        this.expired = status;
        this.killed = status;
        this.replaced = status;
    }
    public void setBridgeReports (boolean status){
        VTBReport = null;
        this.forwarded = status;
        this.intercepted = status;
    }

    public boolean setReplyTo (String q){
        if (q == null || q.isEmpty ())
            return false;
        if (q.startsWith ("queue://") == false)
            JMSReplyTo = "queue://" + q;
        else
            JMSReplyTo = q;
        return true;
    }

    public String getReplyTo (){
        return JMSReplyTo;
    }
    public String getReplyToQueue (){
        return JMSReplyTo.substring ("queue://".length ());
    }

    public Destination getReplyToDestination (){
        return ActiveMQDestination.createDestination (getReplyToQueue(),ActiveMQDestination.TYPE.QUEUE);
    }

    public void setCorrelationID (Object q){
        JMSCorrelationID = q;
    }
    public Object getCorrelationID (){
        return JMSCorrelationID;
    }

    public String getBroker (){return VTBBroker;}
    public String getOriginalId (){return originalId;}
    public String getOriginalAddress (){return originalAddress;}
    public String getOriginalQueue (){return originalQueue;}
    public long getOriginalExpiryTime (){return originalExpiryTime;}
    public long getOriginalTimestamp () {return originalTimestamp;}
    public String getReport () {return VTBReport;}

    @Override
    public String toString (){
        return "ReportFields{event="+VTBReport+",broker="+getBroker()+",originalId="+getOriginalId()+",originalAddress="+getOriginalAddress()
                +",originalQueue="+getOriginalQueue()+",originalExpiryTime="+ getOriginalExpiryTime ()
                +",originalTimestamp="+getOriginalTimestamp()+"}";
    }
    /**
     * Метод для переопределения в наследниках, например, для отправки запросов вообще без отчётов
     * @param message
     * @throws JMSException
     */
    protected void putReportFields (Message message)  throws JMSException{
        if ((delivered || expired || killed || replaced || acknowledged || forwarded || intercepted) == false)
            throw new JMSException ("JMSReport is empty");

        if (VTBReport == null){
            if (delivered)
                VTBReport = "delivered";
            if (expired)
                VTBReport += ((VTBReport==null)?"expired":",expired");
            if (killed)
                VTBReport += ((VTBReport==null)?"killed":",killed");
            if (replaced)
                VTBReport += ((VTBReport==null)?"replaced":",replaced");
            if (acknowledged)
                VTBReport += ((VTBReport==null)?"acknowledged":",acknowledged");
            if (forwarded)
                VTBReport += ((VTBReport==null)?"forwarded":",forwarded");
            if (intercepted)
                VTBReport += ((VTBReport==null)?"intercepted":",intercepted");
        }
        message.setStringProperty ("VTBReport",VTBReport);

        if (referer)
            message.setBooleanProperty ("VTBReferer", true);

        if (VTBAddress != null)
            message.setStringProperty ("VTBAddress",VTBAddress);
    }
/*
    public Message prepareRequest (Message message, final Session session) throws JMSException {

        if (JMSReplyTo == null || JMSReplyTo.isEmpty ())
            throw new JMSException ("JMSReplyTo is empty");

        if (session != null && JMSReplyTo.contains ("::"))
            VTBBroker = ((ClientSessionImpl)((ActiveMQSession)session).getCoreSession()).getConnection().getRemoteAddress ();

        message.setStringProperty ("JMSReplyTo", JMSReplyTo);

        putReportFields (message);

        if (JMSCorrelationID != null)
            message.setObjectProperty ("JMSCorrelationID", JMSCorrelationID);

        return message;
    }
*/
    public Message prepareRequest (Message message) throws JMSException {

        if (JMSReplyTo == null || JMSReplyTo.isEmpty ())
            throw new JMSException ("JMSReplyTo is empty");

        message.setStringProperty ("JMSReplyTo", JMSReplyTo);

        putReportFields (message);

        if (JMSCorrelationID != null)
            message.setObjectProperty ("JMSCorrelationID", JMSCorrelationID);

        return message;
    }
    public static ReportFields from (Message message) throws JMSException {
        String VTBReport = message.getStringProperty ("VTBReport");

        ReportFields r = new ReportFields ();

        if (VTBReport != null && VTBReport.isEmpty () == false){
            if (VTBReport.equals ("delivered"))
                r.delivered = true;
            else if (VTBReport.equals ("expired"))
                r.expired = true;
            else if (VTBReport.equals ("killed"))
                r.killed = true;
            else if (VTBReport.equals ("replaced"))
                r.replaced = true;
            else if (VTBReport.equals ("acknowledged"))
                r.acknowledged = true;
            else if (VTBReport.equals ("forwarded"))
                r.forwarded = true;
            else if (VTBReport.equals ("intercepted"))
                r.intercepted = true;
            else
                throw new JMSException ("VTBReport is empty");
        }
        else
            throw new JMSException ("VTBReport is empty");

        r.VTBReport = VTBReport;
        r.originalAddress = message.getStringProperty ("_AMQ_ORIG_ADDRESS");
        r.JMSReplyTo = message.getStringProperty ("JMSReplyTo");
        r.JMSCorrelationID = message.getObjectProperty ("JMSCorrelationID");
        r.VTBBroker = message.getStringProperty ("VTBBroker");

        if (message.propertyExists ("VTBOriginalMessageId"))
            r.originalId = message.getStringProperty ("VTBOriginalMessageId");

        r.originalQueue = message.getStringProperty ("_AMQ_ORIG_QUEUE");
        r.originalExpiryTime = message.getLongProperty ("_AMQ_ACTUAL_EXPIRY");

        if (message.propertyExists ("VTBOriginalTimestamp"))
            r.originalTimestamp = message.getLongProperty ("VTBOriginalTimestamp");

        return r;
    }
    public boolean isResponseSuitable (Message response) throws JMSException {
        return JMSCorrelationID == null || (response.propertyExists ("JMSCorrelationID")
                && response.getObjectProperty ("JMSCorrelationID").equals (JMSCorrelationID));
    }

    public final String getSelector (){
        return "JMSCorrelationID=\'" + JMSCorrelationID + "\'";
    }
}
