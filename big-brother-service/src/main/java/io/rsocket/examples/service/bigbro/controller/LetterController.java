package io.rsocket.examples.service.bigbro.controller;

import io.rsocket.examples.common.control.Worker;
import io.rsocket.examples.common.model.Letter;
import io.rsocket.examples.service.bigbro.services.GuardService;
import io.rsocket.examples.service.bigbro.services.LetterDecoder;
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
public class LetterController {

	final InstrumentedPool<Worker> pool;
	final LetterDecoder            decoder;
	final GuardService             guardService;

	@MessageMapping("analyse.letter")
	public Mono<Void> processLetter(@Payload Letter letter) {
		log.info("Received Letter={} for decoding", letter);

		return pool.withPoolable(worker -> worker.execute(() -> decoder
			           .decode(letter)
			           .flatMap(guardService::send)
				   ))
		           .then()
		           .onErrorMap(t -> new IllegalStateException("Too Many Requests"));
	}

}
