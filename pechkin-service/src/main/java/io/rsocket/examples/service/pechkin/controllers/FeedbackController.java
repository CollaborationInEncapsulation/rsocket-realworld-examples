package io.rsocket.examples.service.pechkin.controllers;

import io.rsocket.examples.common.model.Notification;
import lombok.extern.slf4j.Slf4j;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class FeedbackController {

    @PostMapping("/letter-status")
    public void feedback(@RequestBody Notification feedback) {
        log.info("feedback = " + feedback);
    }

}
