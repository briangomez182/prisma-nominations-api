package com.prisma.nominations.infrastructure.adapter.in.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Asigna un correlation id a cada request (D13): lo toma de {@code X-Correlation-Id} o lo genera,
 * lo deja en el MDC para que aparezca en cada línea de log y lo devuelve siempre en la respuesta.
 * <p>
 * Solo se acepta un valor acotado ({@code [A-Za-z0-9._-]{1,64}}): un valor arbitrario del cliente
 * terminaría en logs, historial y headers de Kafka (log injection, datos sensibles, tamaño).
 * Si no cumple, se reemplaza por un UUID nuevo en lugar de rechazar el request.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final Pattern VALID_CORRELATION_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = resolve(request.getHeader(ApiHeaders.CORRELATION_ID));
        MDC.put(ApiHeaders.CORRELATION_ID_MDC_KEY, correlationId);
        // Se setea antes de la cadena para que esté presente aunque la respuesta se confirme antes de volver.
        response.setHeader(ApiHeaders.CORRELATION_ID, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(ApiHeaders.CORRELATION_ID_MDC_KEY);
        }
    }

    static String resolve(String candidate) {
        if (candidate != null && VALID_CORRELATION_ID.matcher(candidate).matches()) {
            return candidate;
        }
        return UUID.randomUUID().toString();
    }
}
