package org.hestiastore.index.segmentindex.core.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexNameMdcScopeTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void open_restoresPreviousIndexNameOnClose() {
        MDC.put("index.name", "previous");

        try (IndexNameMdcScope ignored = IndexNameMdcScope.open("next")) {
            assertEquals("next", MDC.get("index.name"));
        }

        assertEquals("previous", MDC.get("index.name"));
    }

    @Test
    void open_supportsNestedScopes() {
        try (IndexNameMdcScope ignored = IndexNameMdcScope.open("outer")) {
            assertEquals("outer", MDC.get("index.name"));
            try (IndexNameMdcScope nested = IndexNameMdcScope.open("inner")) {
                assertEquals("inner", MDC.get("index.name"));
            }
            assertEquals("outer", MDC.get("index.name"));
        }

        assertNull(MDC.get("index.name"));
    }

    @Test
    void openIfConfigured_skipsBlankOrDisabledConfiguration() {
        @SuppressWarnings("unchecked")
        final IndexConfiguration<Integer, String> disabled = mock(
                IndexConfiguration.class);
        @SuppressWarnings("unchecked")
        final IndexConfiguration<Integer, String> blankName = mock(
                IndexConfiguration.class);
        when(disabled.isContextLoggingEnabled()).thenReturn(false);
        when(blankName.isContextLoggingEnabled()).thenReturn(true);
        when(blankName.getIndexName()).thenReturn("   ");

        try (IndexNameMdcScope ignored = IndexNameMdcScope
                .openIfConfigured(disabled)) {
            assertNull(MDC.get("index.name"));
        }
        try (IndexNameMdcScope ignored = IndexNameMdcScope
                .openIfConfigured(blankName)) {
            assertNull(MDC.get("index.name"));
        }
    }
}
