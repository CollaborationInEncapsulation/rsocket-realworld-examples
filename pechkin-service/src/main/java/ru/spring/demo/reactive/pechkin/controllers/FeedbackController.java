package ru.spring.demo.reactive.pechkin.controllers;

import lombok.extern.slf4j.Slf4j;
import ru.spring.demo.reactive.starter.speed.model.Notification;

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
