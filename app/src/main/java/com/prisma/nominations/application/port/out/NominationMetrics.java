package com.prisma.nominations.application.port.out;

import com.prisma.nominations.domain.enums.ChangeSource;
import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.enums.ResolutionOutcome;

/**
 * Métricas de negocio del ciclo de vida de una nominación. Los casos de uso registran hechos de negocio; cómo se
 * exportan (Micrometer → Prometheus) es del adapter, así la aplicación no depende de la librería de métricas.
 * <p>
 * Se invocan <b>después</b> de que la transacción confirmó: un rollback o una reevaluación por lock optimista no
 * deja un conteo de más.
 * <p>
 * <b>Cardinalidad:</b> la única dimensión abierta es {@code entity_id} (decenas de entidades adheridas, acotado).
 * Nunca se etiqueta con nomination_id, request_id ni correlation_id: eso va a logs y trazas.
 */
public interface NominationMetrics {

    /** Resultado de un intento de envío a ABM. */
    enum SubmissionOutcome {
        /** ABM aceptó el pedido. */
        SUBMITTED,
        /** La nominación ya no estaba en RECEIVED: no se llamó a ABM. */
        SKIPPED,
        /** Falla técnica (timeout, 5xx, circuito abierto): la reintenta el ABM Adapter. */
        UNAVAILABLE,
        /** ABM rechazó el pedido por contrato: no reintentable. */
        CONTRACT_ERROR
    }

    /** Alta nueva aceptada (en Prometheus: nominations_received_total). */
    void created(String entityId);

    /** Reintento idempotente del canal (mismo request_id y mismo contenido). */
    void replayed(String entityId);

    /** Mismo request_id con contenido distinto (409). */
    void idempotencyConflict(String entityId);

    void submission(SubmissionOutcome outcome);

    /** Cada respuesta de ABM procesada, incluidas las duplicadas y contradictorias. */
    void abmResponse(ResolutionOutcome outcome);

    /**
     * Nominación que llegó a un estado final (APPROVED/REJECTED). Registra también el tiempo de resolución
     * (created_at → updated_at).
     */
    void resolved(Nomination nomination);

    /** Pasaje a ABM_TIMEOUT: {@link ChangeSource#ABM_ADAPTER} (reintentos agotados) o {@link ChangeSource#SWEEPER}. */
    void abmTimeout(ChangeSource source);

    /** Reproceso manual de una nominación en ABM_TIMEOUT. */
    void reprocessed();

    /** Sin efectos: tests unitarios y armado manual de casos de uso. */
    NominationMetrics NOOP = new NominationMetrics() {
        @Override
        public void created(String entityId) {
        }

        @Override
        public void replayed(String entityId) {
        }

        @Override
        public void idempotencyConflict(String entityId) {
        }

        @Override
        public void submission(SubmissionOutcome outcome) {
        }

        @Override
        public void abmResponse(ResolutionOutcome outcome) {
        }

        @Override
        public void resolved(Nomination nomination) {
        }

        @Override
        public void abmTimeout(ChangeSource source) {
        }

        @Override
        public void reprocessed() {
        }
    };
}
