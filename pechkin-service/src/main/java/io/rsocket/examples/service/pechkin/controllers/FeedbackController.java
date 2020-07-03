package io.rsocket.examples.service.pechkin.controllers;

import io.rsocket.examples.common.model.Notification;
import lombok.extern.slf4j.Slf4j;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Controller
public class FeedbackController {

    @MessageMapping("letter-status")
    public void feedback(@Payload Notification feedback) {
        log.info("feedback = " + feedback);
    }

}
