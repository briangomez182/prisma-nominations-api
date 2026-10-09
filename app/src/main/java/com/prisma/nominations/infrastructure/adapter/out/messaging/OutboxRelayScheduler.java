package com.prisma.nominations.infrastructure.adapter.out.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Ciclo programado del relay. {@code fixedDelay}: un ciclo no arranca hasta que terminó el anterior.
 * Si un lote vino lleno se encadena otro enseguida, para vaciar un backlog sin esperar el delay.
 * Se apaga con {@code nominations.outbox.relay.enabled=false} (tests, o cuando publica Debezium).
 */
@Component
@ConditionalOnProperty(name = "nominations.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    /** Tope de lotes encadenados por ciclo: cede el hilo del scheduler a otras tareas. */
    static final int MAX_BATCHES_PER_RUN = 10;

    private final OutboxRelay relay;
    private final int batchSize;

    OutboxRelayScheduler(OutboxRelay relay, OutboxProperties properties) {
        this.relay = relay;
        this.batchSize = properties.relay().batchSize();
    }

    @Scheduled(fixedDelayString = "${nominations.outbox.relay.fixed-delay}")
    void run() {
        try {
            for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
                if (relay.relayOnce() < batchSize) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            // Base caída u otra falla del ciclo: el próximo reintenta, el scheduler no debe morir.
            log.error("Outbox: falló el ciclo del relay: {}", OutboxRelay.describe(e));
        }
    }
}
