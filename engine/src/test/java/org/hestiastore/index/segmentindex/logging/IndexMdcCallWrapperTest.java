package org.hestiastore.index.segmentindex.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexMdcCallWrapperTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void constructorRejectsBlankIndexName() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexMdcCallWrapper(" "));
        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
    }

    @Test
    void openScopeSetsAndRestoresIndexNameInMdc() {
        final IndexMdcCallWrapper wrapper = new IndexMdcCallWrapper("idx");

        MDC.put("index.name", "outer");
        try (IndexMdcScope ignored = wrapper.openScope()) {
            assertEquals("idx", MDC.get("index.name"));
        }

        assertEquals("outer", MDC.get("index.name"));
    }

    @Test
    void openScopeClearsTemporaryIndexNameWhenNoPreviousValueExists() {
        final IndexMdcCallWrapper wrapper = new IndexMdcCallWrapper("idx");

        try (IndexMdcScope ignored = wrapper.openScope()) {
            assertEquals("idx", MDC.get("index.name"));
        }

        assertNull(MDC.get("index.name"));
    }

    @Test
    void runSetsAndRestoresIndexNameInMdc() {
        final IndexMdcCallWrapper wrapper = new IndexMdcCallWrapper(
                "idx");
        final AtomicReference<String> observed = new AtomicReference<>();

        MDC.put("index.name", "outer");
        wrapper.run(() -> observed.set(MDC.get("index.name")));

        assertEquals("idx", observed.get());
        assertEquals("outer", MDC.get("index.name"));
    }

    @Test
    void supplyClearsTemporaryIndexNameWhenNoPreviousValueExists() {
        final IndexMdcCallWrapper wrapper = new IndexMdcCallWrapper(
                "idx");

        final String value = wrapper.supply(() -> MDC.get("index.name"));

        assertEquals("idx", value);
        assertNull(MDC.get("index.name"));
    }
}
