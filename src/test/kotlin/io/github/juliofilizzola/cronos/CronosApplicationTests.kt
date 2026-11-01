package io.github.juliofilizzola.cronos

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import

@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = ["cronos.security.api-keys=test-key"])
class CronosApplicationTests {

    @Test
    fun contextLoads() {
    }

}
