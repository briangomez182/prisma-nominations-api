package com.prisma.nominations.domain;

final class Masking {

    private Masking() {
    }

    /** Deja visibles solo los últimos 4 caracteres: {@code 987654 → ****7654}. */
    static String lastFour(String value) {
        return value.length() <= 4 ? "****" : "****" + value.substring(value.length() - 4);
    }
}
