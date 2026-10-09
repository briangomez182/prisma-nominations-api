package com.prisma.nominations.domain;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SensitiveDataTest {

    @ParameterizedTest
    @ValueSource(strings = {"4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111", "5500000000000004"})
    @Tag("E2")
    void cardTokenRejectsFullPan(String pan) {
        assertThatThrownBy(() -> new CardToken(pan))
                .isInstanceOf(InvalidNominationDataException.class)
                .hasMessageContaining("número de tarjeta completo");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tok_4f9a2c", "TOKEN_OR_MASKED_REFERENCE", "************1111", "411111******1111"})
    void cardTokenAcceptsTokensAndMaskedReferences(String value) {
        assertThat(new CardToken(value).value()).isEqualTo(value);
    }

    @Test
    void toStringNeverExposesFullValues() {
        assertThat(new CardToken("tok_4f9a2c")).hasToString("CardToken[****9a2c]");
        assertThat(new AccountId("987654")).hasToString("AccountId[****7654]");
    }

    @Test
    @Tag("E2")
    void accountIdRejectsInvalidFormat() {
        assertThatThrownBy(() -> new AccountId("98-76"))
                .isInstanceOf(InvalidNominationDataException.class)
                .extracting(e -> ((InvalidNominationDataException) e).field())
                .isEqualTo("account_id");
    }
}
