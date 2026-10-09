package com.prisma.nominations.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.AuthenticatedEntityArgumentResolver;
import com.prisma.nominations.infrastructure.adapter.in.web.SecurityProblemHandler;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.boot.actuate.metrics.export.prometheus.PrometheusScrapeEndpoint;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authorization.AuthorityAuthorizationManager;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.authorization.AuthorizationManager;
import org.springframework.security.authorization.AuthorizationManagers;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.RequestAuthorizationContext;

import javax.crypto.spec.SecretKeySpec;
import java.time.Instant;
import java.util.Objects;

/**
 * Seguridad por diseño y mínimo privilegio (D12): OAuth2 Resource Server con JWT.
 * <p>
 * <b>Token</b>: {@code sub} = client id del canal, {@code entity_id} = entidad financiera (obligatoria en /v1),
 * {@code scope} = scopes separados por espacio, que se convierten en authorities {@code SCOPE_*}.
 * <p>
 * <b>Autorización</b> (cada ruta pide solo lo que necesita; lo no listado se deniega):
 * <ul>
 *   <li>{@code POST /v1/nominations} → {@code nominations:write} + entidad válida.</li>
 *   <li>{@code GET /v1/nominations/**} → {@code nominations:read} + entidad válida.</li>
 *   <li>{@code /internal/**} y Actuator (salvo health, info y prometheus) → {@code nominations:operate}.</li>
 *   <li>Públicos: health/info (probes de Kubernetes; el detalle solo con operate), prometheus (scraping),
 *       Swagger/OpenAPI y el simulador /abm-mock (no existe en producción).</li>
 * </ul>
 * <b>Demo vs producción</b>: la demo firma con HS256 y una clave simétrica de configuración
 * ({@code nominations.security.jwt}). En producción la app solo valida: issuer-uri/JWKS del IdP corporativo
 * (claves asimétricas rotables, sin secreto compartido), mTLS entre gateway y canales, y Swagger, prometheus y
 * Actuator publicados solo en la red interna (puerto de management separado).
 * <p>
 * Stateless (sin sesión HTTP) y sin CSRF: la API no usa cookies; el token viaja en {@code Authorization}, que el
 * navegador no agrega solo, así que no hay request cross-site que herede credenciales. Se mantienen los headers
 * de seguridad por defecto de Spring Security (nosniff, no-cache, X-Frame-Options, HSTS en HTTPS).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(JwtSecurityProperties.class)
public class SecurityConfig {

    public static final String SCOPE_WRITE = "SCOPE_nominations:write";
    public static final String SCOPE_READ = "SCOPE_nominations:read";
    public static final String SCOPE_OPERATE = "SCOPE_nominations:operate";

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http, JwtDecoder jwtDecoder,
                                            SecurityProblemHandler problemHandler) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .requestCache(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> auth
                        // Probes y scraping (en producción, solo en la red interna / puerto de management)
                        .requestMatchers(EndpointRequest.to(HealthEndpoint.class, InfoEndpoint.class)).permitAll()
                        .requestMatchers(EndpointRequest.to(PrometheusScrapeEndpoint.class)).permitAll()
                        .requestMatchers(EndpointRequest.toAnyEndpoint()).hasAuthority(SCOPE_OPERATE)
                        // Documentación: pública en la demo; en producción se restringe o no se publica
                        .requestMatchers("/v3/api-docs", "/v3/api-docs/**", "/v3/api-docs.yaml",
                                "/swagger-ui.html", "/swagger-ui/**").permitAll()
                        // Simulador del sistema externo ABM: no forma parte de la API (no existe en producción)
                        .requestMatchers("/abm-mock/**").permitAll()
                        .requestMatchers(HttpMethod.POST, "/v1/nominations").access(withEntity(SCOPE_WRITE))
                        .requestMatchers(HttpMethod.GET, "/v1/nominations/**").access(withEntity(SCOPE_READ))
                        // Resto de /v1 (otro método o ruta): sin acceso a datos; MVC responde 404/405
                        .requestMatchers("/v1/**").access(withEntity(SCOPE_READ, SCOPE_WRITE))
                        .requestMatchers("/internal/**").hasAuthority(SCOPE_OPERATE)
                        // Despacho de error del contenedor: lo que llegue acá ya fue autorizado antes
                        .requestMatchers("/error").permitAll()
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.decoder(jwtDecoder).jwtAuthenticationConverter(jwtAuthenticationConverter()))
                        .authenticationEntryPoint(problemHandler)
                        .accessDeniedHandler(problemHandler))
                .exceptionHandling(exceptions -> exceptions
                        .authenticationEntryPoint(problemHandler)
                        .accessDeniedHandler(problemHandler));
        return http.build();
    }

    /** Alguno de los scopes + claim {@code entity_id} válido: un token de /v1 sin entidad no puede operar (403). */
    private static AuthorizationManager<RequestAuthorizationContext> withEntity(String... scopes) {
        AuthorizationManager<RequestAuthorizationContext> hasEntity = (authentication, context) ->
                new AuthorizationDecision(AuthenticatedEntityArgumentResolver.entityOf(authentication.get()).isPresent());
        return AuthorizationManagers.allOf(AuthorityAuthorizationManager.hasAnyAuthority(scopes), hasEntity);
    }

    /** {@code scope: "a b"} → authorities {@code SCOPE_a, SCOPE_b}; el principal es {@code sub}. */
    private static JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtGrantedAuthoritiesConverter scopes = new JwtGrantedAuthoritiesConverter();
        scopes.setAuthoritiesClaimName("scope");
        scopes.setAuthorityPrefix("SCOPE_");
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(scopes);
        converter.setPrincipalClaimName(JwtClaimNames.SUB);
        return converter;
    }

    /**
     * Demo: HS256 con clave simétrica. Valida firma, algoritmo, {@code exp} (obligatorio), {@code nbf} e
     * {@code iss}. Producción: {@code NimbusJwtDecoder.withIssuerLocation(issuerUri)} (JWKS del IdP).
     */
    @Bean
    JwtDecoder jwtDecoder(JwtSecurityProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder
                .withSecretKey(new SecretKeySpec(properties.secretBytes(), "HmacSHA256"))
                .macAlgorithm(MacAlgorithm.HS256)
                .build();
        OAuth2TokenValidator<Jwt> expRequired = new JwtClaimValidator<Instant>(JwtClaimNames.EXP, Objects::nonNull);
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(properties.issuer()), expRequired));
        return decoder;
    }

    @Bean
    SecurityProblemHandler securityProblemHandler(ObjectMapper objectMapper) {
        return new SecurityProblemHandler(objectMapper);
    }
}
