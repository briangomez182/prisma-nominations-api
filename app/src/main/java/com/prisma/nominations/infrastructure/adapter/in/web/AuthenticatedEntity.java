package com.prisma.nominations.infrastructure.adapter.in.web;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Inyecta en un parámetro {@code String} la entidad financiera del token (claim {@code entity_id}).
 * <p>
 * La entidad sale siempre del JWT, nunca de un header o del body: así una entidad no puede operar sobre
 * nominaciones de otra. Lo resuelve {@link AuthenticatedEntityArgumentResolver}.
 */
@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthenticatedEntity {
}
