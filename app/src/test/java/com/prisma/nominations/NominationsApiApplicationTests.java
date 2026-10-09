package com.prisma.nominations;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class NominationsApiApplicationTests {

    @Test
    void contextLoads() {
    }
}
