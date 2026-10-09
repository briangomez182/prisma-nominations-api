package com.prisma.nominations.infrastructure.adapter.in.web;

import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.StatusChange;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Historial de auditoría de una nominación, en orden cronológico. */
public record NominationHistoryResponse(UUID nominationId, List<Item> items) {

    public static NominationHistoryResponse from(UUID nominationId, List<StatusChange> changes) {
        return new NominationHistoryResponse(nominationId, changes.stream().map(Item::from).toList());
    }

    /** @param fromStatus {@code null} (omitido) en la creación */
    @Schema(name = "NominationHistoryItem", description = "Una transición de estado")
    public record Item(
            @Schema(description = "Estado anterior; ausente en la creación")
            NominationStatus fromStatus,
            NominationStatus toStatus,
            ChangeSource source,
            String detail,
            String correlationId,
            Instant occurredAt) {

        static Item from(StatusChange change) {
            return new Item(change.from(), change.to(), change.source(), change.detail(),
                    change.correlationId(), change.occurredAt());
        }
    }
}
