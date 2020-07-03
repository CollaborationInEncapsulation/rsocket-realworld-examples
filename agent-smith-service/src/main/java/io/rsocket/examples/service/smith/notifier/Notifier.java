package io.rsocket.examples.service.smith.notifier;

import io.rsocket.examples.common.model.Notification;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Notifier {

	final RSocketRequester rSocketRequester;

	public Mono<Void> sendNotification(Notification notification) {
		return rSocketRequester
                .route("letter-status")
                .data(notification)
                .send()
                .doOnSuccess(__ -> log.info("Guard notification sent"))
                .doOnError(e -> log.error("no sender url found", e));
	}
}
