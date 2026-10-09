package com.prisma.nominations.infrastructure.adapter.in.web;

/**
 * Ejemplos de la documentación OpenAPI. Datos ficticios: card_id es siempre un token, nunca un PAN.
 */
final class OpenApiExamples {

    static final String PROBLEM_SCHEMA_REF = "#/components/schemas/ProblemDetail";
    static final String PROBLEM_JSON = "application/problem+json";

    static final String CREATE_REQUEST = """
            {
              "request_id": "0b4a9f2e-6c1d-4e7a-8b3f-5d2c1a0e9f87",
              "customer_id": "CUST-000123",
              "account_id": "0001234567890987654",
              "card_id": "tok_4f9a2c7b8d1e",
              "alias": "CUENTA SUELDO"
            }""";

    static final String NOMINATION = """
            {
              "nomination_id": "7d1e6c2a-4b8f-4a51-9c3e-2f6a8b0d1e23",
              "request_id": "0b4a9f2e-6c1d-4e7a-8b3f-5d2c1a0e9f87",
              "status": "RECEIVED",
              "customer_id": "CUST-000123",
              "account_id": "****7654",
              "card_id": "****8d1e",
              "alias": "CUENTA SUELDO",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "created_at": "2026-10-08T12:00:00Z",
              "updated_at": "2026-10-08T12:00:00Z"
            }""";

    static final String HISTORY = """
            {
              "nomination_id": "7d1e6c2a-4b8f-4a51-9c3e-2f6a8b0d1e23",
              "items": [
                {
                  "to_status": "RECEIVED",
                  "source": "API",
                  "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
                  "occurred_at": "2026-10-08T12:00:00Z"
                }
              ]
            }""";

    static final String VALIDATION_ERROR = """
            {
              "type": "https://api.prisma.example/problems/validation-error",
              "title": "Datos inválidos",
              "status": 400,
              "detail": "La solicitud contiene datos inválidos",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "VALIDATION_ERROR",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z",
              "errors": [
                {"field": "card_id", "message": "card_id parece un número de tarjeta completo: se espera un token o una referencia enmascarada"}
              ]
            }""";

    static final String MALFORMED_REQUEST = """
            {
              "type": "https://api.prisma.example/problems/malformed-request",
              "title": "Solicitud mal formada",
              "status": 400,
              "detail": "El cuerpo de la solicitud no es JSON válido o tiene campos con tipo incorrecto",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "MALFORMED_REQUEST",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z",
              "errors": [{"field": "request_id", "message": "Tipo o formato inválido"}]
            }""";

    static final String MISSING_HEADER = """
            {
              "type": "https://api.prisma.example/problems/missing-header",
              "title": "Header obligatorio ausente",
              "status": 400,
              "detail": "Falta el header obligatorio X-Entity-Id",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "MISSING_HEADER",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z"
            }""";

    static final String INVALID_PARAMETER = """
            {
              "type": "https://api.prisma.example/problems/invalid-parameter",
              "title": "Parámetro inválido",
              "status": 400,
              "detail": "Un parámetro de la solicitud tiene formato inválido",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "INVALID_PARAMETER",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z",
              "errors": [{"field": "nomination_id", "message": "Formato inválido"}]
            }""";

    static final String NOT_FOUND = """
            {
              "type": "https://api.prisma.example/problems/nomination-not-found",
              "title": "Nominación inexistente",
              "status": 404,
              "detail": "No existe la nominación solicitada",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "NOMINATION_NOT_FOUND",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z"
            }""";

    static final String IDEMPOTENCY_CONFLICT = """
            {
              "type": "https://api.prisma.example/problems/idempotency-conflict",
              "title": "Conflicto de idempotencia",
              "status": 409,
              "detail": "El request_id ya fue utilizado con datos distintos",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "IDEMPOTENCY_CONFLICT",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z"
            }""";

    static final String NOMINATION_REPROCESSED = """
            {
              "nomination_id": "7d1e6c2a-4b8f-4a51-9c3e-2f6a8b0d1e23",
              "request_id": "0b4a9f2e-6c1d-4e7a-8b3f-5d2c1a0e9f87",
              "status": "RECEIVED",
              "customer_id": "CUST-000123",
              "account_id": "****7654",
              "card_id": "****8d1e",
              "alias": "CUENTA SUELDO",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "created_at": "2026-10-08T12:00:00Z",
              "updated_at": "2026-10-08T12:40:00Z"
            }""";

    static final String INVALID_STATE_TRANSITION = """
            {
              "type": "https://api.prisma.example/problems/invalid-state-transition",
              "title": "Estado no válido para la operación",
              "status": 409,
              "detail": "La nominación no está en un estado que admita esta operación",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "INVALID_STATE_TRANSITION",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z"
            }""";

    static final String CONCURRENT_MODIFICATION = """
            {
              "type": "https://api.prisma.example/problems/concurrent-modification",
              "title": "Modificación concurrente",
              "status": 409,
              "detail": "El recurso fue modificado por otra operación; reintentar",
              "instance": "urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001",
              "code": "CONCURRENT_MODIFICATION",
              "correlation_id": "c0ffee00-1234-4abc-9def-000000000001",
              "timestamp": "2026-10-08T12:00:00Z"
            }""";

    private OpenApiExamples() {
    }
}
