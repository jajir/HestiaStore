package org.hestiastore.index.segmentindex.core.lifecycle;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexRuntimeViewContextLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void snapshotRunsWithIndexContext() {
        final IndexRuntimeView delegate = mock(IndexRuntimeView.class);
        final IndexRuntimeSnapshot snapshot = mock(IndexRuntimeSnapshot.class);
        final AtomicReference<String> observedMdc = new AtomicReference<>();

        when(delegate.snapshot()).thenAnswer(invocation -> {
            observedMdc.set(MDC.get("index.name"));
            return snapshot;
        });

        final IndexRuntimeViewContextLoggingAdapter adapter =
                new IndexRuntimeViewContextLoggingAdapter(delegate,
                        new IndexContextScopeRunner("idx"));

        assertSame(snapshot, adapter.snapshot());
        assertSame("idx", observedMdc.get());
    }
}
