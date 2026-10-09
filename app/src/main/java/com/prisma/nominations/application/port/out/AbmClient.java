package com.prisma.nominations.application.port.out;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;

/**
 * Envío de una nominación a ABM. ABM responde de forma asincrónica (por abm.responses.v1): este
 * puerto solo confirma que ABM aceptó el pedido.
 * <p>
 * ABM es idempotente por nomination_id: reenviar el mismo pedido devuelve el mismo abm_operation_id.
 */
public interface AbmClient {

    /**
     * @return abm_operation_id asignado por ABM
     * @throws AbmUnavailableException falla técnica (timeout, 5xx, 408, 429, conexión): reintentable
     * @throws AbmContractException    ABM rechazó el pedido por contrato (resto de 4xx): no reintentable
     */
    String submit(AbmRequest request);
}
