package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class EntryIteratorLoggingContextTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void setsAndClearsMdcForOperations() {
        final IndexConfiguration<String, String> conf = mock(
                IndexConfiguration.class);
        when(conf.getIndexName()).thenReturn("idx");

        final CapturingIterator iterator = new CapturingIterator();
        final EntryIteratorLoggingContext<String, String> loggingContext = new EntryIteratorLoggingContext<>(
                iterator, conf);

        assertTrue(loggingContext.hasNext());
        assertEquals("idx", iterator.mdcAtHasNext);
        assertNull(MDC.get("index.name"));

        final Entry<String, String> entry = loggingContext.next();
        assertEquals(Entry.of("key", "value"), entry);
        assertEquals("idx", iterator.mdcAtNext);
        assertNull(MDC.get("index.name"));

        loggingContext.close();
        assertEquals("idx", iterator.mdcAtClose);
        assertTrue(iterator.closed);
        assertNull(MDC.get("index.name"));
    }

    private static final class CapturingIterator
            implements EntryIterator<String, String> {

        private String mdcAtHasNext;
        private String mdcAtNext;
        private String mdcAtClose;
        private boolean closed;

        @Override
        public boolean hasNext() {
            mdcAtHasNext = MDC.get("index.name");
            return true;
        }

        @Override
        public Entry<String, String> next() {
            mdcAtNext = MDC.get("index.name");
            return Entry.of("key", "value");
        }

        @Override
        public void close() {
            mdcAtClose = MDC.get("index.name");
            closed = true;
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }
    }
}
