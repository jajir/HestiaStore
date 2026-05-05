package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimeconfiguration.ConfigurationSnapshot;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchResult;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchValidation;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
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
    private SegmentIndex<String, String> delegate;

    @Mock
    private SegmentIndexMaintenance maintenance;

    private IndexContextLoggingAdapter<String, String> adapter;

    @BeforeEach
    void setUp() {
        when(delegate.runtimeConfiguration()).thenReturn(mock(RuntimeConfiguration.class));
        when(delegate.runtimeMonitoring()).thenReturn(mock(IndexRuntimeMonitoring.class));
        when(delegate.maintenance()).thenReturn(maintenance);
        adapter = new IndexContextLoggingAdapter<>(delegate, new IndexMdcScopeRunner("idx"));
    }

    @AfterEach
    void tearDown() {
        if (adapter != null && !adapter.wasClosed()) {
            adapter.close();
        }
        MDC.clear();
    }

    @Test
    void constructorRejectsBlankIndexName() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexContextLoggingAdapter<>(delegate,
                        new IndexMdcScopeRunner("  ")));
        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
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

    @Test
    void wrapsRuntimeConfigurationAndRuntimeMonitoringApisWithMdc() {
        final RuntimeConfiguration delegateRuntimeConfiguration = mock(
                RuntimeConfiguration.class);
        final IndexRuntimeMonitoring delegateRuntime = mock(IndexRuntimeMonitoring.class);
        final SegmentIndexMaintenance delegateMaintenance = mock(
                SegmentIndexMaintenance.class);
        final RuntimeConfigPatch patch = mock(RuntimeConfigPatch.class);

        final AtomicReference<String> mdcAtRuntimeSnapshot = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetActual = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetOriginal = new AtomicReference<>();
        final AtomicReference<String> mdcAtValidate = new AtomicReference<>();
        final AtomicReference<String> mdcAtApply = new AtomicReference<>();

        when(delegate.runtimeConfiguration()).thenReturn(delegateRuntimeConfiguration);
        when(delegate.runtimeMonitoring()).thenReturn(delegateRuntime);
        when(delegate.maintenance()).thenReturn(delegateMaintenance);
        when(delegateRuntime.snapshot()).thenAnswer(invocation -> {
            mdcAtRuntimeSnapshot.set(MDC.get("index.name"));
            return mock(IndexRuntimeSnapshot.class);
        });
        when(delegateRuntimeConfiguration.getCurrent())
                .thenAnswer(invocation -> {
                    mdcAtGetActual.set(MDC.get("index.name"));
                    return mock(ConfigurationSnapshot.class);
                });
        when(delegateRuntimeConfiguration.getOriginal())
                .thenAnswer(invocation -> {
                    mdcAtGetOriginal.set(MDC.get("index.name"));
                    return mock(ConfigurationSnapshot.class);
                });
        when(delegateRuntimeConfiguration.validate(patch)).thenAnswer(invocation -> {
            mdcAtValidate.set(MDC.get("index.name"));
            return mock(RuntimePatchValidation.class);
        });
        when(delegateRuntimeConfiguration.apply(patch)).thenAnswer(invocation -> {
            mdcAtApply.set(MDC.get("index.name"));
            return mock(RuntimePatchResult.class);
        });
        adapter = new IndexContextLoggingAdapter<>(delegate, new IndexMdcScopeRunner("idx"));

        final RuntimeConfiguration wrappedRuntimeConfiguration = adapter.runtimeConfiguration();
        assertSame(wrappedRuntimeConfiguration, adapter.runtimeConfiguration());
        adapter.runtimeMonitoring().snapshot();
        wrappedRuntimeConfiguration.getCurrent();
        wrappedRuntimeConfiguration.getOriginal();
        wrappedRuntimeConfiguration.validate(patch);
        wrappedRuntimeConfiguration.apply(patch);

        assertEquals("idx", mdcAtRuntimeSnapshot.get());
        assertEquals("idx", mdcAtGetActual.get());
        assertEquals("idx", mdcAtGetOriginal.get());
        assertEquals("idx", mdcAtValidate.get());
        assertEquals("idx", mdcAtApply.get());
        assertNull(MDC.get("index.name"));
    }

    @Test
    void restoresPreviousMdcForSegmentIndexDataFacade() {
        final AtomicReference<String> mdcAtCompact = new AtomicReference<>();
        final AtomicReference<String> mdcAtCompactAndWait = new AtomicReference<>();
        final AtomicReference<String> mdcAtFlush = new AtomicReference<>();
        final AtomicReference<String> mdcAtFlushAndWait = new AtomicReference<>();
        final AtomicReference<String> mdcAtCheck = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetStream = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetStreamIsolation = new AtomicReference<>();
        final AtomicReference<String> mdcAtPutEntry = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetStreamDefault = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetStreamDefaultIsolation = new AtomicReference<>();
        final SegmentWindow window = SegmentWindow.unbounded();
        final Entry<String, String> entry = Entry.of("entry-key", "entry-val");

        doAnswer(invocation -> {
            mdcAtCompact.set(MDC.get("index.name"));
            return null;
        }).when(maintenance).compact();
        doAnswer(invocation -> {
            mdcAtCompactAndWait.set(MDC.get("index.name"));
            return null;
        }).when(maintenance).compactAndWait();
        doAnswer(invocation -> {
            mdcAtFlush.set(MDC.get("index.name"));
            return null;
        }).when(maintenance).flush();
        doAnswer(invocation -> {
            mdcAtFlushAndWait.set(MDC.get("index.name"));
            return null;
        }).when(maintenance).flushAndWait();
        doAnswer(invocation -> {
            mdcAtCheck.set(MDC.get("index.name"));
            return null;
        }).when(maintenance).checkAndRepairConsistency();
        when(delegate.getStream(window)).thenAnswer(invocation -> {
            mdcAtGetStream.set(MDC.get("index.name"));
            return Stream.empty();
        });
        when(delegate.getStream(window, SegmentIteratorIsolation.FULL_ISOLATION))
                .thenAnswer(invocation -> {
                    mdcAtGetStreamIsolation.set(MDC.get("index.name"));
                    return Stream.empty();
                });
        doAnswer(invocation -> {
            mdcAtPutEntry.set(MDC.get("index.name"));
            return null;
        }).when(delegate).put(entry);
        when(delegate.getStream()).thenAnswer(invocation -> {
            mdcAtGetStreamDefault.set(MDC.get("index.name"));
            return Stream.empty();
        });
        when(delegate.getStream(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenAnswer(invocation -> {
                    mdcAtGetStreamDefaultIsolation.set(MDC.get("index.name"));
                    return Stream.empty();
                });

        MDC.put("index.name", "outer");
        adapter.maintenance().compact();
        assertEquals("outer", MDC.get("index.name"));
        adapter.maintenance().compactAndWait();
        assertEquals("outer", MDC.get("index.name"));
        adapter.maintenance().flush();
        assertEquals("outer", MDC.get("index.name"));
        adapter.maintenance().flushAndWait();
        assertEquals("outer", MDC.get("index.name"));
        adapter.maintenance().checkAndRepairConsistency();
        assertEquals("outer", MDC.get("index.name"));
        adapter.put(entry);
        assertEquals("outer", MDC.get("index.name"));
        try (Stream<?> ignored = adapter.getStream(window)) {
            assertEquals("outer", MDC.get("index.name"));
        }
        try (Stream<?> ignored = adapter.getStream(window,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals("outer", MDC.get("index.name"));
        }
        try (Stream<?> ignored = adapter.getStream()) {
            assertEquals("outer", MDC.get("index.name"));
        }
        try (Stream<?> ignored = adapter
                .getStream(SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals("outer", MDC.get("index.name"));
        }

        assertDelegatedMdcValues(mdcAtCompact, mdcAtCompactAndWait, mdcAtFlush,
                mdcAtFlushAndWait, mdcAtCheck, mdcAtPutEntry, mdcAtGetStream,
                mdcAtGetStreamIsolation, mdcAtGetStreamDefault,
                mdcAtGetStreamDefaultIsolation);
    }

    @SafeVarargs
    private static void assertDelegatedMdcValues(
            final AtomicReference<String>... mdcValues) {
        for (final AtomicReference<String> mdcValue : mdcValues) {
            assertEquals("idx", mdcValue.get());
        }
    }
}
