package org.hestiastore.index.segmentindex.core.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexContextScopeRunnerTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void constructorRejectsBlankIndexName() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexContextScopeRunner(" "));
        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
    }

    @Test
    void runSetsAndRestoresIndexNameInMdc() {
        final IndexContextScopeRunner runner = new IndexContextScopeRunner(
                "idx");
        final AtomicReference<String> observed = new AtomicReference<>();

        MDC.put("index.name", "outer");
        runner.run(() -> observed.set(MDC.get("index.name")));

        assertEquals("idx", observed.get());
        assertEquals("outer", MDC.get("index.name"));
    }

    @Test
    void supplyClearsTemporaryIndexNameWhenNoPreviousValueExists() {
        final IndexContextScopeRunner runner = new IndexContextScopeRunner(
                "idx");

        final String value = runner.supply(() -> MDC.get("index.name"));

        assertEquals("idx", value);
        assertNull(MDC.get("index.name"));
    }
}
