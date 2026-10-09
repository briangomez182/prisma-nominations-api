package com.prisma.nominations.application.service;

import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.ResolutionOutcome;

import java.util.ArrayList;
import java.util.List;

/** Fake de métricas: registra cada llamada como texto ("created:ENT01", "submission:SUBMITTED"...) para verificarla. */
class RecordingNominationMetrics implements NominationMetrics {

    final List<String> calls = new ArrayList<>();
    final List<Nomination> resolved = new ArrayList<>();

    @Override
    public void created(String entityId) {
        calls.add("created:" + entityId);
    }

    @Override
    public void replayed(String entityId) {
        calls.add("replayed:" + entityId);
    }

    @Override
    public void idempotencyConflict(String entityId) {
        calls.add("idempotencyConflict:" + entityId);
    }

    @Override
    public void submission(SubmissionOutcome outcome) {
        calls.add("submission:" + outcome);
    }

    @Override
    public void abmResponse(ResolutionOutcome outcome) {
        calls.add("abmResponse:" + outcome);
    }

    @Override
    public void resolved(Nomination nomination) {
        calls.add("resolved:" + nomination.status());
        resolved.add(nomination);
    }

    @Override
    public void abmTimeout(ChangeSource source) {
        calls.add("abmTimeout:" + source);
    }

    @Override
    public void reprocessed() {
        calls.add("reprocessed");
    }
}
