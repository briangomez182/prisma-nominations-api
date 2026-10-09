package com.prisma.nominations.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Base64;

/**
 * Validación de JWT de la demo ({@code nominations.security.jwt}): clave HS256 simétrica y issuer esperado.
 * <p>
 * Se valida al arrancar: sin clave, con base64 inválido o con menos de 256 bits la app no levanta (fail fast,
 * en lugar de aceptar una clave débil). En producción se reemplaza por el JWKS del IdP (ver SecurityConfig).
 *
 * @param secret clave en base64 (≥ 32 bytes); NO productiva, sobreescribible con NOMINATIONS_SECURITY_JWT_SECRET
 * @param issuer valor exigido en el claim {@code iss}
 */
@ConfigurationProperties(prefix = "nominations.security.jwt")
public record JwtSecurityProperties(String secret, String issuer) {

    private static final int MIN_KEY_BYTES = 32;

    public JwtSecurityProperties {
        if (issuer == null || issuer.isBlank()) {
            throw new IllegalArgumentException("nominations.security.jwt.issuer es obligatorio");
        }
        if (secretBytes(secret).length < MIN_KEY_BYTES) {
            throw new IllegalArgumentException("nominations.security.jwt.secret debe tener al menos 256 bits");
        }
    }

    /** Clave decodificada. El mensaje de error nunca incluye el valor configurado. */
    public byte[] secretBytes() {
        return secretBytes(secret);
    }

    private static byte[] secretBytes(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("nominations.security.jwt.secret es obligatorio");
        }
        try {
            return Base64.getDecoder().decode(secret.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("nominations.security.jwt.secret no es base64 válido");
        }
    }

    /** El record no debe imprimir la clave en logs ni en mensajes de error. */
    @Override
    public String toString() {
        return "JwtSecurityProperties[secret=****, issuer=" + issuer + "]";
    }
}
