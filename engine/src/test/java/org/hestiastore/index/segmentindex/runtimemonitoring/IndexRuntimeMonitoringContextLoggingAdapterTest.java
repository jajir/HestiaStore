package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
class IndexRuntimeMonitoringContextLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void snapshotRunsWithIndexContext() {
        final IndexRuntimeMonitoring delegate = mock(
                IndexRuntimeMonitoring.class);
        final IndexRuntimeSnapshot snapshot = mock(IndexRuntimeSnapshot.class);
        final AtomicReference<String> observedMdc = new AtomicReference<>();
        when(delegate.snapshot()).thenAnswer(invocation -> {
            observedMdc.set(MDC.get("index.name"));
            return snapshot;
        });

        final IndexRuntimeMonitoringContextLoggingAdapter adapter =
                new IndexRuntimeMonitoringContextLoggingAdapter(delegate,
                        new IndexMdcScopeRunner("idx"));

        assertSame(snapshot, adapter.snapshot());
        assertEquals("idx", observedMdc.get());
    }
}
