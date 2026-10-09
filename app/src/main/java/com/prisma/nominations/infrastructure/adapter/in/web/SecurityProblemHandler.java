package com.prisma.nominations.infrastructure.adapter.in.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.GlobalExceptionHandler.ProblemCode;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.access.AccessDeniedHandler;

import java.io.IOException;

/**
 * 401 y 403 de la cadena de seguridad con el mismo formato de error que el resto de la API (RFC 9457,
 * {@code code} UNAUTHORIZED / FORBIDDEN, {@code correlation_id}). El correlation id ya está en el MDC:
 * {@link CorrelationIdFilter} corre antes que la cadena de Spring Security.
 * <p>
 * Nunca se expone el motivo concreto del rechazo del token (vencido, firma, issuer): solo el código de error
 * estándar de RFC 6750 en {@code WWW-Authenticate}. El detalle queda en el log (clase de la excepción).
 */
public class SecurityProblemHandler implements AuthenticationEntryPoint, AccessDeniedHandler {

    private final ObjectMapper objectMapper;

    public SecurityProblemHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** Sin token o token inválido → 401 + {@code WWW-Authenticate: Bearer [error="invalid_token"]}. */
    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException ex)
            throws IOException {
        String challenge = ex instanceof OAuth2AuthenticationException oauth
                ? "Bearer error=\"" + oauth.getError().getErrorCode() + "\""
                : "Bearer";
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, challenge);
        write(response, GlobalExceptionHandler.securityProblem(ProblemCode.UNAUTHORIZED, ex));
    }

    /** Token válido sin el scope o la entidad que exige la ruta → 403 ({@code insufficient_scope}, RFC 6750). */
    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, AccessDeniedException ex)
            throws IOException {
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer error=\"insufficient_scope\"");
        write(response, GlobalExceptionHandler.securityProblem(ProblemCode.FORBIDDEN, ex));
    }

    private void write(HttpServletResponse response, ProblemDetail problem) throws IOException {
        response.setStatus(problem.getStatus());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        objectMapper.writeValue(response.getOutputStream(), problem);
    }
}
