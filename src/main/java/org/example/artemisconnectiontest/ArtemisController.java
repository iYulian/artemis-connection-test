package org.example.artemisconnectiontest;

import org.example.artemisconnectiontest.vtbartemis.adapter.VtbArtemisSender;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.JMSException;

@RestController
@RequestMapping(path = "/")
public class ArtemisController {
    private final VtbArtemisSender vtbArtemisSender;

    public ArtemisController(VtbArtemisSender vtbArtemisSender) {
        this.vtbArtemisSender = vtbArtemisSender;
    }

    @GetMapping
    public void artemis() throws JMSException {
        vtbArtemisSender.send();
    }


}
