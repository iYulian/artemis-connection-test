package org.example.artemisconnectiontest.utils;

import org.springframework.jms.core.MessagePostProcessor;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class ResponseTemplate extends RequestID implements MessagePostProcessor {

    ResponseTemplate(RequestID clone) {
        super (clone.id, clone.queue, clone.expiration, clone.user, clone.broker, clone.correlation);
    }

    public static ResponseTemplate from (File f) throws IOException {
        RequestID id = RequestID.from (f);
        if (id == null)
            return null;
        return new ResponseTemplate (id);
    }
    public static ResponseTemplate from (InputStream i) throws IOException {
        RequestID id = RequestID.from (i);
        if (id == null)
            return null;
        return new ResponseTemplate (RequestID.from (i));
    }

    public static ResponseTemplate from (String s){
        RequestID id = RequestID.from (s);
        if (id == null)
            return null;
        return new ResponseTemplate (RequestID.from (s));
    }

    @Override
    public Message postProcessMessage (Message response) throws JMSException {
        prepareResponse (response);
        return response;
    }
}
