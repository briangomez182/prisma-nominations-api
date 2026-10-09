package com.prisma.nominations.infrastructure.adapter.in.web;

import org.springframework.core.MethodParameter;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Resuelve {@link AuthenticatedEntity}: lee y valida el claim {@code entity_id} del JWT autenticado.
 * <p>
 * La cadena de seguridad ya rechaza con 403 un token de /v1 sin entidad válida ({@link #entityOf}); esta
 * validación es la segunda barrera (defensa en profundidad) por si una ruta nueva quedara mal configurada.
 */
public class AuthenticatedEntityArgumentResolver implements HandlerMethodArgumentResolver {

    /** Claim con la entidad financiera del canal que llama. */
    public static final String ENTITY_CLAIM = "entity_id";

    /** Acotado: termina en la clave de idempotencia, logs y eventos. */
    private static final Pattern VALID_ENTITY = Pattern.compile("[A-Za-z0-9_-]{1,20}");

    /** Entidad del token si el claim existe, es un string y cumple el patrón. */
    public static Optional<String> entityOf(Authentication authentication) {
        if (authentication instanceof JwtAuthenticationToken token) {
            return entityOf(token.getToken());
        }
        return Optional.empty();
    }

    static Optional<String> entityOf(Jwt jwt) {
        return jwt.getClaims().get(ENTITY_CLAIM) instanceof String entity && VALID_ENTITY.matcher(entity).matches()
                ? Optional.of(entity)
                : Optional.empty();
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.hasParameterAnnotation(AuthenticatedEntity.class)
                && String.class.equals(parameter.getParameterType());
    }

    @Override
    public String resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                  NativeWebRequest webRequest, WebDataBinderFactory binderFactory) {
        return entityOf(SecurityContextHolder.getContext().getAuthentication())
                .orElseThrow(() -> new AccessDeniedException("Token sin entidad válida"));
    }
}
