/**
 * Casos de uso y puertos.
 * <ul>
 *   <li>{@code port.in}: lo que la aplicación ofrece (crear nominación, procesar respuesta de ABM, consultar).</li>
 *   <li>{@code port.out}: lo que la aplicación necesita (repositorio, outbox, cliente ABM).</li>
 *   <li>{@code service}: implementación de los casos de uso y límites transaccionales.</li>
 * </ul>
 * Depende solo de {@code domain}.
 */
package com.prisma.nominations.application;
