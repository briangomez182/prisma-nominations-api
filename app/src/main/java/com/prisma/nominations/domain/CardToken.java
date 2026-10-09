package com.prisma.nominations.domain;

import java.util.regex.Pattern;

/**
 * Referencia a la tarjeta: token emitido por la entidad o referencia enmascarada. Nunca el PAN.
 * <p>
 * Si el valor tiene forma de PAN (13 a 19 dígitos) se rechaza: la plataforma no recibe ni persiste
 * números de tarjeta completos y queda fuera del alcance PCI.
 */
public record CardToken(String value) {

    private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9_*-]{4,64}");
    private static final Pattern PAN_LIKE = Pattern.compile("\\d{13,19}");

    public CardToken {
        if (value == null || value.isBlank()) {
            throw new InvalidNominationDataException("card_id", "card_id es obligatorio");
        }
        if (PAN_LIKE.matcher(value.replaceAll("[\\s-]", "")).matches()) {
            throw new InvalidNominationDataException("card_id",
                    "card_id parece un número de tarjeta completo: se espera un token o una referencia enmascarada");
        }
        if (!ALLOWED.matcher(value).matches()) {
            throw new InvalidNominationDataException("card_id",
                    "card_id debe tener entre 4 y 64 caracteres alfanuméricos, '_', '-' o '*'");
        }
    }

    public String masked() {
        return Masking.lastFour(value);
    }

    @Override
    public String toString() {
        return "CardToken[" + masked() + "]";
    }
}
