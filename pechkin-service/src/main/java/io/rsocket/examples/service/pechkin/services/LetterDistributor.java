package io.rsocket.examples.service.pechkin.services;

import java.util.function.Function;

import io.rsocket.examples.common.control.Worker;
import io.rsocket.examples.service.pechkin.producer.LetterProducer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Service;

/**
 * @author Evgeny Borisov
 */
@Slf4j
@Getter
@Service
@RequiredArgsConstructor
public class LetterDistributor extends BaseSubscriber<Void>
		implements InitializingBean, DisposableBean {

	final LetterProducer                   producer;
	final RSocketRequester                 rSocketRequester;
	final InstrumentedPool<Worker>         pool;
	final Function<Mono<Void>, Mono<Void>> responseHandlerFunction;

	@Override
	public void afterPropertiesSet() {
	    int concurrency = pool.metrics()
	                .getMaxAllocatedSize() + pool.metrics()
	                                             .getMaxPendingAcquireSize();
		producer.letterFlux()
		        .flatMap(letter -> pool
			        .withPoolable(worker -> worker.execute(() ->
				        rSocketRequester
					        .route("analyse.letter")
					        .data(letter)
					        .send()
					        .transform(responseHandlerFunction)
			        )),
			        concurrency
		        )
		        .subscribe(this);
	}

	@Override
	public void destroy() {
		dispose();
	}
}
