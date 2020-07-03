package io.rsocket.examples.common.control;

import java.time.Duration;
import java.util.function.Function;

import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;
import reactor.pool.PooledRef;
import reactor.util.context.Context;

@RequiredArgsConstructor
public class WaitFreeInstrumentedPool<POOLABLE> implements InstrumentedPool<POOLABLE> {

	final InstrumentedPool<POOLABLE> origin;
	final OverflowStrategy           overflowStrategy;

	@Override
	public PoolMetrics metrics() {
		return origin.metrics();
	}

	public <T> Flux<T> withPoolable(Function<POOLABLE, Publisher<T>> scopeFunction) {
		return origin
				.withPoolable(scopeFunction)
				.doOnError(__ -> {
					if (overflowStrategy == OverflowStrategy.TERMINATE) {
						dispose();
					}
				});
	}

	@Override
	public Mono<Integer> warmup() {
		return origin.warmup();
	}

	@Override
	public Mono<PooledRef<POOLABLE>> acquire() {
		return origin.acquire();
	}

	@Override
	public Mono<PooledRef<POOLABLE>> acquire(Duration timeout) {
		return origin.acquire(timeout);
	}

	@Override
	public Mono<Void> disposeLater() {
		return origin.disposeLater();
	}

	@Override
	public boolean isDisposed() {
		return origin.isDisposed();
	}
}
