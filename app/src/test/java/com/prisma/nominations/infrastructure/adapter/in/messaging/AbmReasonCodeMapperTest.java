package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.prisma.nominations.domain.RejectionReason;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class AbmReasonCodeMapperTest {

    @ParameterizedTest
    @CsvSource({
            "ABM-010, INVALID_ACCOUNT",
            "ABM-020, INVALID_CARD",
            "ABM-030, CARD_NOT_ELIGIBLE",
            "ABM-051, ACCOUNT_BLOCKED",
            "ABM-060, ALREADY_NOMINATED"})
    void knownCodesAreNormalized(String code, RejectionReason expected) {
        assertThat(AbmReasonCodeMapper.toRejectionReason(code)).isEqualTo(expected);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"ABM-999", "abm-051", "051", "ABM-051\nINYECCION"})
    void unknownCodesAreOther(String code) {
        assertThat(AbmReasonCodeMapper.toRejectionReason(code)).isEqualTo(RejectionReason.OTHER);
    }
}
