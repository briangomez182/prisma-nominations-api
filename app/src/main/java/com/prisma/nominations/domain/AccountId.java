package com.prisma.nominations.domain;

import java.util.regex.Pattern;

/**
 * Identificador de cuenta. Se persiste porque ABM lo necesita, pero se expone y se loguea siempre
 * enmascarado: {@link #toString()} nunca devuelve el valor completo.
 */
public record AccountId(String value) {

    private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9]{4,34}");

    public AccountId {
        if (value == null || value.isBlank()) {
            throw new InvalidNominationDataException("account_id", "account_id es obligatorio");
        }
        if (!ALLOWED.matcher(value).matches()) {
            throw new InvalidNominationDataException("account_id",
                    "account_id debe tener entre 4 y 34 caracteres alfanuméricos");
        }
    }

    public String masked() {
        return Masking.lastFour(value);
    }

    @Override
    public String toString() {
        return "AccountId[" + masked() + "]";
    }
}
