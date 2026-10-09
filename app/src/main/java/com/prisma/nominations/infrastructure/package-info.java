/**
 * Adaptadores de los puertos.
 * <ul>
 *   <li>{@code adapter.in.web}: API REST.</li>
 *   <li>{@code adapter.in.messaging}: consumers de Kafka (pedidos a ABM, respuestas de ABM).</li>
 *   <li>{@code adapter.out.persistence}: JPA/PostgreSQL y tabla outbox.</li>
 *   <li>{@code adapter.out.messaging}: relay del outbox hacia Kafka.</li>
 *   <li>{@code adapter.out.abm}: cliente HTTP de ABM (y su mock en el perfil {@code abm-mock}).</li>
 *   <li>{@code config}: configuración transversal (seguridad, Kafka, observabilidad).</li>
 * </ul>
 */
package com.prisma.nominations.infrastructure;
