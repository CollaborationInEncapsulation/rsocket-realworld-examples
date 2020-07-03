package io.rsocket.examples.starter.speed.configuration;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import com.hazelcast.core.HazelcastInstance;
import com.netflix.concurrency.limits.limit.VegasLimit;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.rsocket.core.LeaseConfig;
import io.rsocket.examples.common.control.ErrorStrategy;
import io.rsocket.examples.common.control.LeaseManager;
import io.rsocket.examples.common.control.LimitBasedLeaseSender;
import io.rsocket.examples.common.control.OverflowStrategy;
import io.rsocket.examples.common.control.SimpleWorker;
import io.rsocket.examples.common.control.WaitFreeInstrumentedPool;
import io.rsocket.examples.common.control.Worker;
import io.rsocket.examples.common.processing.Delayer;
import io.rsocket.examples.starter.speed.AdjustmentProperties;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;
import reactor.util.retry.Retry;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;

/**
 * @author Evgeny Borisov
 * @author Kirill Tolkachev
 * @author Oleh Dokuka
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(AdjustmentProperties.class)
public class BaseConfiguration {

	@Bean
	public RSocketRequester rSocketRequester(RSocketRequester.Builder rsocketRequesterBuilder,
			AdjustmentProperties adjustmentProperties) {

		return rsocketRequesterBuilder
			.rsocketConnector(connector ->
				connector
					.lease(LeaseConfig::deferOnNoLease)
					.reconnect(
						Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
						     .maxBackoff(Duration.ofSeconds(5))
					)
			)
			.websocket(URI.create(adjustmentProperties.getReceiver().getBaseUrl() + "rsocket"));
	}

	@Bean
	public RSocketServerCustomizer rSocketServerCustomizer(AdjustmentProperties properties, LeaseManager leaseManager) {
		return rSocketServer -> rSocketServer
				.lease((config) -> {
					final LimitBasedLeaseSender senderAndStatsCollector =
							new LimitBasedLeaseSender(
									UUID.randomUUID().toString(),
									leaseManager,
									VegasLimit.newBuilder()
									          .initialLimit(properties.getProcessing().getConcurrencyLevel())
									          .maxConcurrency(properties.getProcessing().getQueueSize())
									          .build());
					config.sender(senderAndStatsCollector)
					      .statsCollector(senderAndStatsCollector);
				});
	}

	@Bean
	public LeaseManager leaseManager(AdjustmentProperties properties) {
		AdjustmentProperties.ProcessingProperties processingProperties =
				properties.getProcessing();
		return new LeaseManager(processingProperties.getConcurrencyLevel(), (int) processingProperties.getTime());
	}

	@Bean
	public Supplier<Worker> rateLimitedWorker(AdjustmentProperties adjustmentProperties, Optional<HazelcastInstance> hazelcastInstanceOptional) {
		return SimpleWorker::new;
	}

	@Bean
	public InstrumentedPool<Worker> letterProcessorExecutor(AdjustmentProperties adjustmentProperties, Supplier<Worker> workerSupplier) {
		AdjustmentProperties.ProcessingProperties processingProperties =
				adjustmentProperties.getProcessing();
		InstrumentedPool<Worker> pool =
			PoolBuilder
				.from(Mono.fromSupplier(workerSupplier))
				.maxPendingAcquire(processingProperties.getQueueSize())
				.sizeBetween(processingProperties.getConcurrencyLevel(), processingProperties.getConcurrencyLevel())
				.evictionPredicate((wc, metadata) -> false)
				.lifo();

		if (processingProperties.getOverflowStrategy() == OverflowStrategy.BLOCK) {
			return pool;
		}
		return new WaitFreeInstrumentedPool<>(pool, processingProperties.getOverflowStrategy());
	}

	@Bean
	public Function<Mono<Void>, Mono<Void>> responseHandlerFunction(
		    AdjustmentProperties properties,
			@Qualifier("letter.status") AtomicBoolean status,
			MeterRegistry meterRegistry) {

		Counter speedCounter = meterRegistry.counter("letter.rps");
		Counter retriesCounter = meterRegistry.counter("letter.fps");
		Counter dropsCounter = meterRegistry.counter("letter.dps");
		ErrorStrategy errorStrategy = properties.getSender().getErrorStrategy();

		return mono -> mono
			.retryWhen(
				Retry.backoff(10, Duration.ofMillis(100))
				     .maxBackoff(Duration.ofSeconds(5))
				     .filter(t -> (t.getMessage() != null && t.getMessage().contains("Connection refused")) || errorStrategy == ErrorStrategy.RETRY)
				     .doBeforeRetry(rs -> {
					     if (rs.failure().getMessage() != null && rs.failure().getMessage().contains("Connection refused")) {
						     return;
					     }
					     retriesCounter.increment();
				         log.info("Failed. Retrying. {}", rs);
				     })
			)
			.doOnSubscribe(__ -> speedCounter.increment())
			.doOnSuccess(__ -> log.info("Letter processed and sent"))
			.onErrorResume(e -> {
				switch (errorStrategy) {
					case DROP:
					case RETRY:
						dropsCounter.increment();
						return Mono.empty();
				}

				status.set(false);
				log.error("Letter processed but was not sent, terminate app ", e);
				return Mono.error(e);
			});
	}

	@Bean
	public Delayer delayer(AdjustmentProperties adjustmentProperties) {
		return new Delayer(
			adjustmentProperties.getProcessing().getTime(),
			adjustmentProperties.getProcessing().getRandomDelay()
		);
	}
}
