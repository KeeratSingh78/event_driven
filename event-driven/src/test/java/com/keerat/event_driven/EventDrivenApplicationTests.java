package com.keerat.event_driven;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Application context integration test.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>The Spring ApplicationContext starts successfully</li>
 *   <li>PostgreSQL connectivity is established</li>
 *   <li>Flyway migrations (V1, V2, V3) are applied</li>
 *   <li>Hibernate validates all JPA entities against the database</li>
 * </ul>
 */
@SpringBootTest
@ActiveProfiles("test")
class EventDrivenApplicationTests {

    @Test
    void contextLoads() {
    }
}
