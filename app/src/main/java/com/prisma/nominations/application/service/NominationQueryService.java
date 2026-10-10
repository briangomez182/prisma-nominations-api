package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.GetNominationQuery;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.model.StatusChange;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Consultas acotadas a la entidad. Una nominación de otra entidad responde igual que una inexistente.
 */
@Service
class NominationQueryService implements GetNominationQuery {

    private final NominationRepository repository;

    NominationQueryService(NominationRepository repository) {
        this.repository = repository;
    }

    @Override
    public Nomination get(String entityId, UUID nominationId) {
        return repository.findById(nominationId)
                .filter(n -> n.entityId().equals(entityId))
                .orElseThrow(() -> new NominationNotFoundException(nominationId));
    }

    @Override
    public List<StatusChange> history(String entityId, UUID nominationId) {
        get(entityId, nominationId);
        return repository.findHistory(nominationId);
    }
}
