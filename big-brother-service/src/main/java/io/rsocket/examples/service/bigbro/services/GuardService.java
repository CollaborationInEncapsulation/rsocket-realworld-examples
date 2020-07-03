package io.rsocket.examples.service.bigbro.services;

import java.util.function.Function;

import io.rsocket.examples.common.model.DecodedLetter;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class GuardService {

	final Function<Mono<Void>, Mono<Void>> responseHandlerFunction;
	final RSocketRequester                 rSocketRequester;

	public Mono<Void> send(DecodedLetter decodedLetter) {
		GuardRequest request = new GuardRequest().setLetterId(decodedLetter.getAuthor())
		                                         .setMessage(decodedLetter.getContent());

		return rSocketRequester
				.route("guard")
				.data(request)
				.send()
				.transform(responseHandlerFunction);
	}

	@Data
	@Accessors(chain = true)
	@NoArgsConstructor
	public static class GuardRequest {

		private String letterId;
		private String message;
	}
}