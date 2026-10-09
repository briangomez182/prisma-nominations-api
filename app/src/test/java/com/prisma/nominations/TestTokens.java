package com.prisma.nominations;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;

/**
 * Tokens para tests, con el mismo formato que {@code scripts/mint-token.sh}.
 * <ul>
 *   <li>{@link #bearer}: JWT HS256 real firmado con la clave de demo de application.yml, para tests que pasan por
 *       el {@code JwtDecoder} real (RANDOM_PORT con RestClient, o MockMvc con header Authorization).</li>
 *   <li>{@link #jwt}: {@code jwt()} de spring-security-test, para {@code @WebMvcTest} (no decodifica nada).</li>
 * </ul>
 */
public final class TestTokens {

    public static final String WRITE = "nominations:write";
    public static final String READ = "nominations:read";
    public static final String OPERATE = "nominations:operate";

    /** Igual a nominations.security.jwt.secret / issuer de application.yml (clave de demo, no productiva). */
    public static final String DEMO_SECRET = "PaMXbHK/6wehI0TgMBIYHUm7EjsB+82m10nGxpXDpYE=";
    public static final String DEMO_ISSUER = "prisma-nominations-demo";

    private TestTokens() {
    }

    /** {@code "Bearer <jwt>"} de un canal de la entidad con read + write (lo habitual en los tests de API). */
    public static String bearer(String entityId) {
        return bearer(entityId, WRITE, READ);
    }

    public static String bearer(String entityId, String... scopes) {
        return "Bearer " + token().entity(entityId).scopes(scopes).mint();
    }

    /** Operador: solo {@code nominations:operate}, sin entidad. */
    public static String operatorBearer() {
        return "Bearer " + token().scopes(OPERATE).mint();
    }

    public static Builder token() {
        return new Builder();
    }

    /** MockMvc sin decodificar: claims {@code entity_id} y {@code scope}; authorities SCOPE_* derivadas del scope. */
    public static JwtRequestPostProcessor jwt(String entityId, String... scopes) {
        return SecurityMockMvcRequestPostProcessors.jwt().jwt(jwt -> {
            jwt.subject("test-channel").claim("scope", String.join(" ", scopes));
            if (entityId != null) {
                jwt.claim("entity_id", entityId);
            }
        });
    }

    /** Variantes para los casos negativos (vencido, otra clave, otro issuer, sin entidad). */
    public static final class Builder {

        private String subject = "test-channel";
        private String entityId;
        private String scope = "";
        private String issuer = DEMO_ISSUER;
        private String secret = DEMO_SECRET;
        private Instant expiresAt = Instant.now().plus(Duration.ofHours(1));

        public Builder entity(String entityId) {
            this.entityId = entityId;
            return this;
        }

        public Builder scopes(String... scopes) {
            this.scope = String.join(" ", scopes);
            return this;
        }

        public Builder issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Builder secret(String base64Secret) {
            this.secret = base64Secret;
            return this;
        }

        /** {@code null} = token sin {@code exp} (debe rechazarse). */
        public Builder expiresAt(Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public String mint() {
            JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder()
                    .subject(subject)
                    .issuer(issuer)
                    .issueTime(Date.from(Instant.now().minusSeconds(5)))
                    .expirationTime(expiresAt != null ? Date.from(expiresAt) : null)
                    .claim("scope", scope);
            if (entityId != null) {
                claims.claim("entity_id", entityId);
            }
            try {
                SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims.build());
                jwt.sign(new MACSigner(Base64.getDecoder().decode(secret)));
                return jwt.serialize();
            } catch (JOSEException e) {
                throw new IllegalStateException(e);
            }
        }

        public String bearer() {
            return "Bearer " + mint();
        }
    }
}
