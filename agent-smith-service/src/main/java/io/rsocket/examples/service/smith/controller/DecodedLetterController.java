package io.rsocket.examples.service.smith.controller;

import io.rsocket.examples.common.control.Worker;
import io.rsocket.examples.common.model.DecodedLetter;
import io.rsocket.examples.service.smith.decider.GuardDecider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

@Slf4j
@Controller
@RequiredArgsConstructor
public class DecodedLetterController {

	final GuardDecider             decider;
	final InstrumentedPool<Worker> pool;

	@MessageMapping("guard")
	public Mono<Void> updateLetterStatus(@Payload DecodedLetter decodedLetter) {
		log.info("Received DecodedLetter={} for analysis", decodedLetter);

		return pool.withPoolable(worker -> worker.execute(() -> decider.decide(decodedLetter)))
		           .then()
		           .onErrorMap(t -> new IllegalStateException("Too Many Requests"));
	}

}