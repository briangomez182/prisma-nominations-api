package com.prisma.nominations;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * El contexto carga con la configuración de la app. Sin ABM Adapter: sin servidor HTTP no hay a quién llamar
 * (mismas properties que {@code NominationCommandServiceConcurrencyTest}: comparten contexto). El contexto completo
 * con ABM lo levanta {@link AbmFlowIntegrationTest}.
 */
@SpringBootTest(properties = "nominations.abm.adapter.enabled=false")
@Import(TestcontainersConfiguration.class)
class NominationsApiApplicationTests {

    @Test
    void contextLoads() {
    }
}
