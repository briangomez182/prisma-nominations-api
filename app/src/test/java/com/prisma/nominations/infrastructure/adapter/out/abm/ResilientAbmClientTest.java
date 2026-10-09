package com.prisma.nominations.infrastructure.adapter.out.abm;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Decorador de resiliencia con registries propios (mismas reglas que application.yml, tiempos chicos) y un
 * {@link AbmClient} fake que cuenta invocaciones.
 */
class ResilientAbmClientTest {

    private static final Duration OPEN_WAIT = Duration.ofMillis(100);
    private static final Duration SLOW_CALL = Duration.ofMillis(50);

    private final FakeAbmClient fake = new FakeAbmClient();
    private final CircuitBreakerRegistry cbRegistry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(4)
            .minimumNumberOfCalls(4)
            .failureRateThreshold(50)
            .slowCallDurationThreshold(SLOW_CALL)
            .slowCallRateThreshold(80)
            .waitDurationInOpenState(OPEN_WAIT)
            .permittedNumberOfCallsInHalfOpenState(2)
            .recordExceptions(AbmUnavailableException.class)
            .ignoreExceptions(AbmContractException.class)
            .build());
    private final ResilientAbmClient client = new ResilientAbmClient(fake, retryRegistry(), cbRegistry);
    private final CircuitBreaker cb = cbRegistry.circuitBreaker(ResilientAbmClient.INSTANCE);

    static RetryRegistry retryRegistry() {
        return RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(10))
                .retryExceptions(AbmUnavailableException.class)
                .ignoreExceptions(AbmContractException.class)
                .build());
    }

    @Test
    @DisplayName("falla transitoria que se recupera en el 2º intento → éxito con 2 llamadas")
    void transientFailure_recoversOnSecondAttempt() {
        fake.then(unavailable()).then(() -> "ABM-OP-1");

        assertThat(client.submit(request())).isEqualTo("ABM-OP-1");
        assertThat(fake.calls()).isEqualTo(2);
        assertThat(cb.getMetrics().getNumberOfFailedCalls()).isEqualTo(1);
        assertThat(cb.getMetrics().getNumberOfSuccessfulCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("3 fallas técnicas → AbmUnavailableException tras 3 llamadas (reintentos agotados)")
    void persistentFailure_exhaustsRetries() {
        fake.always(unavailable());

        assertThatThrownBy(() -> client.submit(request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("HTTP 503");
        assertThat(fake.calls()).isEqualTo(3);
        assertThat(cb.getMetrics().getNumberOfFailedCalls()).isEqualTo(3); // cada intento cuenta para el CB
    }

    @Test
    @DisplayName("error de contrato → 1 sola llamada, sin reintento y sin contar como falla del CB")
    void contractError_isNotRetriedNorRecorded() {
        fake.always(() -> {
            throw new AbmContractException("ABM rechazó el pedido por contrato: HTTP 422");
        });

        assertThatThrownBy(() -> client.submit(request())).isInstanceOf(AbmContractException.class);
        assertThat(fake.calls()).isEqualTo(1);
        assertThat(cb.getMetrics().getNumberOfBufferedCalls()).isZero();
        assertThat(cb.getMetrics().getNumberOfFailedCalls()).isZero();
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("CB abierto: corta el lote en curso y las llamadas siguientes fallan sin invocar a ABM")
    void openCircuit_failsFastWithoutCallingAbm() {
        fake.always(unavailable());

        assertThatThrownBy(() -> client.submit(request())).isInstanceOf(AbmUnavailableException.class);
        assertThat(fake.calls()).isEqualTo(3);

        // 4ª falla → mínimo alcanzado con 100% de fallas → OPEN; el 2º intento de este lote ya no llega a ABM.
        assertThatThrownBy(() -> client.submit(request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("circuit breaker abierto")
                .hasCauseInstanceOf(CallNotPermittedException.class);
        assertThat(fake.calls()).isEqualTo(4);
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        assertThatThrownBy(() -> client.submit(request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("circuit breaker abierto");
        assertThat(fake.calls()).isEqualTo(4);
    }

    @Test
    @DisplayName("half-open: pasado el wait-duration, llamadas sanas cierran el CB")
    void halfOpen_recovers() throws Exception {
        fake.always(unavailable());
        for (int i = 0; i < 2; i++) {
            assertThatThrownBy(() -> client.submit(request())).isInstanceOf(AbmUnavailableException.class);
        }
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        int callsWhileOpen = fake.calls();

        Thread.sleep(OPEN_WAIT.toMillis() + 50);
        fake.always(() -> "ABM-OP-OK");

        assertThat(client.submit(request())).isEqualTo("ABM-OP-OK");
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        assertThat(client.submit(request())).isEqualTo("ABM-OP-OK");
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(fake.calls()).isEqualTo(callsWhileOpen + 2);
    }

    @Test
    @DisplayName("2xx más lento que el slow-call threshold → éxito, sin reintento, contado como slow call")
    void slowSuccess_countsAsSlowCall() {
        fake.always(() -> {
            sleep(SLOW_CALL.multipliedBy(2));
            return "ABM-OP-SLOW";
        });

        assertThat(client.submit(request())).isEqualTo("ABM-OP-SLOW");
        assertThat(fake.calls()).isEqualTo(1);
        assertThat(cb.getMetrics().getNumberOfSlowSuccessfulCalls()).isEqualTo(1);
        assertThat(cb.getMetrics().getNumberOfFailedCalls()).isZero();
    }

    // ---------------------------------------------------------------- helpers

    private static Supplier<String> unavailable() {
        return () -> {
            throw new AbmUnavailableException("ABM respondió HTTP 503", null);
        };
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static AbmRequest request() {
        return new AbmRequest(UUID.randomUUID(), UUID.randomUUID(), "corr-res-1", "0072", "CUST-000123",
                "0720000088000037654321", "tok_4f9a2c7b8d1e", "CUENTA SUELDO");
    }

    /** Respuestas programadas en orden; la última se repite. */
    private static final class FakeAbmClient implements AbmClient {
        private final Deque<Supplier<String>> script = new ArrayDeque<>();
        private final AtomicInteger calls = new AtomicInteger();

        FakeAbmClient then(Supplier<String> behavior) {
            script.addLast(behavior);
            return this;
        }

        void always(Supplier<String> behavior) {
            script.clear();
            script.addLast(behavior);
        }

        int calls() {
            return calls.get();
        }

        @Override
        public String submit(AbmRequest request) {
            calls.incrementAndGet();
            Supplier<String> next = script.size() > 1 ? script.pollFirst() : script.peekFirst();
            return next.get();
        }
    }
}
