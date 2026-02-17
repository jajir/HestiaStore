package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
class IndexContextLoggingAdapterTest {

    @Mock
    private IndexConfiguration<String, String> conf;

    @Mock
    private SegmentIndex<String, String> delegate;

    private IndexContextLoggingAdapter<String, String> adapter;

    @BeforeEach
    void setUp() {
        when(conf.getIndexName()).thenReturn("idx");
        adapter = new IndexContextLoggingAdapter<>(conf, delegate);
    }

    @AfterEach
    void tearDown() {
        if (adapter != null && !adapter.wasClosed()) {
            adapter.close();
        }
        MDC.clear();
    }

    @Test
    void setsAndClearsMdcForDelegatedOperations() {
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
