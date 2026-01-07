package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexContextLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void setsAndClearsMdcForDelegatedOperations() {
        final IndexConfiguration<String, String> conf = mock(
                IndexConfiguration.class);
        when(conf.getIndexName()).thenReturn("idx");

        final SegmentIndex<String, String> delegate = mock(SegmentIndex.class);
        final AtomicReference<String> mdcAtPut = new AtomicReference<>();
        final AtomicReference<String> mdcAtGet = new AtomicReference<>();
        final AtomicReference<String> mdcAtClose = new AtomicReference<>();

        doAnswer(invocation -> {
            mdcAtPut.set(MDC.get("index.name"));
            return null;
        }).when(delegate).put("key", "value");

        when(delegate.get("key")).thenAnswer(invocation -> {
            mdcAtGet.set(MDC.get("index.name"));
            return "value";
        });

        doAnswer(invocation -> {
            mdcAtClose.set(MDC.get("index.name"));
            return null;
        }).when(delegate).close();

        final IndexContextLoggingAdapter<String, String> adapter = new IndexContextLoggingAdapter<>(
                conf, delegate);

        adapter.put("key", "value");
        assertEquals("idx", mdcAtPut.get());
        assertNull(MDC.get("index.name"));

        adapter.get("key");
        assertEquals("idx", mdcAtGet.get());
        assertNull(MDC.get("index.name"));

        adapter.close();
        assertEquals("idx", mdcAtClose.get());
        assertNull(MDC.get("index.name"));
    }
}
