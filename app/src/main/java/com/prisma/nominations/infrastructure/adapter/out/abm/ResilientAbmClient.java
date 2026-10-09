package com.prisma.nominations.infrastructure.adapter.out.abm;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Capa 1 de resiliencia hacia ABM, en proceso: decora a {@link AbmHttpClient} (que aporta el timeout de conexión y
 * de lectura) con un Retry corto y un CircuitBreaker de Resilience4j (instancias {@code abm} de application.yml).
 * La capa 2 (reintentos no bloqueantes por tópicos de retry de Kafka, DLT y ABM_TIMEOUT) vive en el listener: por
 * eso esta capa es corta y solo absorbe blips de red.
 * <p>
 * <b>Orden: Retry(CircuitBreaker(http)).</b> Con el Retry afuera, cada intento HTTP real es una muestra del CB:
 * <ul>
 *   <li>la tasa de fallas refleja llamadas reales, y ante una caída el CB abre antes (cada intento fallido cuenta);</li>
 *   <li>el umbral de slow-call (3s) se mide sobre una llamada HTTP, no sobre un lote que incluye los waits del
 *       backoff (con el CB afuera, tres intentos rápidos que fallan podrían "parecer" una llamada lenta);</li>
 *   <li>si el CB abre a mitad de un lote, el intento siguiente recibe {@link CallNotPermittedException}, que no está
 *       en {@code retry-exceptions}: el Retry corta en el acto en vez de seguir golpeando a ABM.</li>
 * </ul>
 * <b>Qué se reintenta:</b> solo {@link AbmUnavailableException} (timeout, 5xx, 408, 429, conexión). {@link
 * AbmContractException} (4xx de contrato, 2xx sin abm_operation_id) no se reintenta y el CB la ignora: es un error
 * del pedido, no una señal de que ABM esté caído.
 * <p>
 * <b>CB abierto:</b> {@link CallNotPermittedException} se traduce a {@link AbmUnavailableException} ("circuit breaker
 * abierto") sin llamar a ABM; la capa 2 la trata como cualquier falla técnica.
 */
@Primary
@Component
class ResilientAbmClient implements AbmClient {

    static final String INSTANCE = "abm";

    private static final Logger log = LoggerFactory.getLogger(ResilientAbmClient.class);

    private final AbmClient delegate;
    private final Retry retry;
    private final CircuitBreaker circuitBreaker;

    @Autowired
    ResilientAbmClient(AbmHttpClient delegate, RetryRegistry retryRegistry, CircuitBreakerRegistry cbRegistry) {
        this((AbmClient) delegate, retryRegistry, cbRegistry);
    }

    /** Para tests: cualquier {@link AbmClient} como delegado. */
    ResilientAbmClient(AbmClient delegate, RetryRegistry retryRegistry, CircuitBreakerRegistry cbRegistry) {
        this.delegate = delegate;
        this.retry = retryRegistry.retry(INSTANCE);
        this.circuitBreaker = cbRegistry.circuitBreaker(INSTANCE);
        this.circuitBreaker.getEventPublisher().onStateTransition(ResilientAbmClient::logTransition);
    }

    @Override
    public String submit(AbmRequest request) {
        Supplier<String> guarded = CircuitBreaker.decorateSupplier(circuitBreaker, () -> delegate.submit(request));
        Supplier<String> withRetry = Retry.decorateSupplier(retry, logRetries(request, guarded));
        try {
            return withRetry.get();
        } catch (CallNotPermittedException e) {
            log.warn("ABM no invocado, circuit breaker abierto: nomination_id={}, estado={}",
                    request.nominationId(), circuitBreaker.getState());
            throw new AbmUnavailableException("ABM no disponible: circuit breaker abierto (estado "
                    + circuitBreaker.getState() + ")", e);
        }
    }

    /** WARN antes de cada reintento con el número de intento y la causa del anterior (mensajes sin datos sensibles). */
    private Supplier<String> logRetries(AbmRequest request, Supplier<String> call) {
        int maxAttempts = retry.getRetryConfig().getMaxAttempts();
        AtomicInteger attempt = new AtomicInteger();
        AtomicReference<RuntimeException> previous = new AtomicReference<>();
        return () -> {
            int n = attempt.incrementAndGet();
            if (n > 1) {
                log.warn("Reintentando envío a ABM: nomination_id={}, intento={}/{}, causa_anterior={}",
                        request.nominationId(), n, maxAttempts, describe(previous.get()));
            }
            try {
                return call.get();
            } catch (RuntimeException e) {
                previous.set(e);
                throw e;
            }
        };
    }

    private static void logTransition(CircuitBreakerOnStateTransitionEvent event) {
        var transition = event.getStateTransition();
        switch (transition.getToState()) {
            case OPEN, FORCED_OPEN -> log.error("Circuit breaker de ABM {} -> {}: se dejan de enviar pedidos a ABM",
                    transition.getFromState(), transition.getToState());
            case HALF_OPEN -> log.warn("Circuit breaker de ABM {} -> {}: se prueban algunas llamadas",
                    transition.getFromState(), transition.getToState());
            default -> log.warn("Circuit breaker de ABM {} -> {}",
                    transition.getFromState(), transition.getToState());
        }
    }

    private static String describe(RuntimeException e) {
        return e == null ? "-" : e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
