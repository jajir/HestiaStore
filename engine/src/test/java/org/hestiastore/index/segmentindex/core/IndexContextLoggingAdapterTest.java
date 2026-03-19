package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
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
    void constructorRejectsBlankIndexName() {
        final IndexConfiguration<String, String> blankConf = mock(
                IndexConfiguration.class);
        when(blankConf.getIndexName()).thenReturn("  ");
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexContextLoggingAdapter<>(blankConf, delegate));
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
    void wrapsControlPlaneNestedApisWithMdc() {
        final IndexControlPlane delegateControlPlane = mock(
                IndexControlPlane.class);
        final IndexRuntimeView delegateRuntime = mock(IndexRuntimeView.class);
        final IndexConfigurationManagement delegateConfiguration = mock(
                IndexConfigurationManagement.class);
        final RuntimeConfigPatch patch = mock(RuntimeConfigPatch.class);

        final AtomicReference<String> mdcAtIndexName = new AtomicReference<>();
        final AtomicReference<String> mdcAtRuntimeSnapshot = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetActual = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetOriginal = new AtomicReference<>();
        final AtomicReference<String> mdcAtValidate = new AtomicReference<>();
        final AtomicReference<String> mdcAtApply = new AtomicReference<>();

        when(delegate.controlPlane()).thenReturn(delegateControlPlane);
        when(delegateControlPlane.indexName()).thenAnswer(invocation -> {
            mdcAtIndexName.set(MDC.get("index.name"));
            return "idx";
        });
        when(delegateControlPlane.runtime()).thenReturn(delegateRuntime);
        when(delegateRuntime.snapshot()).thenAnswer(invocation -> {
            mdcAtRuntimeSnapshot.set(MDC.get("index.name"));
            return mock(IndexRuntimeSnapshot.class);
        });
        when(delegateControlPlane.configuration())
                .thenReturn(delegateConfiguration);
        when(delegateConfiguration.getConfigurationActual())
                .thenAnswer(invocation -> {
                    mdcAtGetActual.set(MDC.get("index.name"));
                    return mock(ConfigurationSnapshot.class);
                });
        when(delegateConfiguration.getConfigurationOriginal())
                .thenAnswer(invocation -> {
                    mdcAtGetOriginal.set(MDC.get("index.name"));
                    return mock(ConfigurationSnapshot.class);
                });
        when(delegateConfiguration.validate(patch)).thenAnswer(invocation -> {
            mdcAtValidate.set(MDC.get("index.name"));
            return mock(RuntimePatchValidation.class);
        });
        when(delegateConfiguration.apply(patch)).thenAnswer(invocation -> {
            mdcAtApply.set(MDC.get("index.name"));
            return mock(RuntimePatchResult.class);
        });

        final IndexControlPlane wrappedControlPlane = adapter.controlPlane();
        wrappedControlPlane.indexName();
        wrappedControlPlane.runtime().snapshot();
        wrappedControlPlane.configuration().getConfigurationActual();
        wrappedControlPlane.configuration().getConfigurationOriginal();
        wrappedControlPlane.configuration().validate(patch);
        wrappedControlPlane.configuration().apply(patch);

        assertEquals("idx", mdcAtIndexName.get());
        assertEquals("idx", mdcAtRuntimeSnapshot.get());
        assertEquals("idx", mdcAtGetActual.get());
        assertEquals("idx", mdcAtGetOriginal.get());
        assertEquals("idx", mdcAtValidate.get());
        assertEquals("idx", mdcAtApply.get());
        assertNull(MDC.get("index.name"));
    }

    @Test
    void restoresPreviousMdcForSegmentIndexOperations() {
        final AtomicReference<String> mdcAtCompact = new AtomicReference<>();
        final AtomicReference<String> mdcAtCompactAndWait = new AtomicReference<>();
        final AtomicReference<String> mdcAtFlush = new AtomicReference<>();
        final AtomicReference<String> mdcAtFlushAndWait = new AtomicReference<>();
        final AtomicReference<String> mdcAtCheck = new AtomicReference<>();
        final AtomicReference<String> mdcAtPutAsync = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetAsync = new AtomicReference<>();
        final AtomicReference<String> mdcAtDeleteAsync = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetConfiguration = new AtomicReference<>();
        final AtomicReference<String> mdcAtGetState = new AtomicReference<>();
        final AtomicReference<String> mdcAtMetricsSnapshot = new AtomicReference<>();
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
        }).when(delegate).compact();
        doAnswer(invocation -> {
            mdcAtCompactAndWait.set(MDC.get("index.name"));
            return null;
        }).when(delegate).compactAndWait();
        doAnswer(invocation -> {
            mdcAtFlush.set(MDC.get("index.name"));
            return null;
        }).when(delegate).flush();
        doAnswer(invocation -> {
            mdcAtFlushAndWait.set(MDC.get("index.name"));
            return null;
        }).when(delegate).flushAndWait();
        doAnswer(invocation -> {
            mdcAtCheck.set(MDC.get("index.name"));
            return null;
        }).when(delegate).checkAndRepairConsistency();
        when(delegate.putAsync("k", "v")).thenAnswer(invocation -> {
            mdcAtPutAsync.set(MDC.get("index.name"));
            return CompletableFuture.completedFuture(null);
        });
        when(delegate.getAsync("k")).thenAnswer(invocation -> {
            mdcAtGetAsync.set(MDC.get("index.name"));
            return CompletableFuture.completedFuture("v");
        });
        when(delegate.deleteAsync("k")).thenAnswer(invocation -> {
            mdcAtDeleteAsync.set(MDC.get("index.name"));
            return CompletableFuture.completedFuture(null);
        });
        when(delegate.getConfiguration()).thenAnswer(invocation -> {
            mdcAtGetConfiguration.set(MDC.get("index.name"));
            return conf;
        });
        when(delegate.getState()).thenAnswer(invocation -> {
            mdcAtGetState.set(MDC.get("index.name"));
            return SegmentIndexState.READY;
        });
        when(delegate.metricsSnapshot()).thenAnswer(invocation -> {
            mdcAtMetricsSnapshot.set(MDC.get("index.name"));
            return null;
        });
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
        adapter.compact();
        assertEquals("outer", MDC.get("index.name"));
        adapter.compactAndWait();
        assertEquals("outer", MDC.get("index.name"));
        adapter.flush();
        assertEquals("outer", MDC.get("index.name"));
        adapter.flushAndWait();
        assertEquals("outer", MDC.get("index.name"));
        adapter.checkAndRepairConsistency();
        assertEquals("outer", MDC.get("index.name"));
        adapter.putAsync("k", "v");
        assertEquals("outer", MDC.get("index.name"));
        adapter.getAsync("k");
        assertEquals("outer", MDC.get("index.name"));
        adapter.deleteAsync("k");
        assertEquals("outer", MDC.get("index.name"));
        adapter.getConfiguration();
        assertEquals("outer", MDC.get("index.name"));
        adapter.getState();
        assertEquals("outer", MDC.get("index.name"));
        adapter.metricsSnapshot();
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

        assertEquals("idx", mdcAtCompact.get());
        assertEquals("idx", mdcAtCompactAndWait.get());
        assertEquals("idx", mdcAtFlush.get());
        assertEquals("idx", mdcAtFlushAndWait.get());
        assertEquals("idx", mdcAtCheck.get());
        assertEquals("idx", mdcAtPutAsync.get());
        assertEquals("idx", mdcAtGetAsync.get());
        assertEquals("idx", mdcAtDeleteAsync.get());
        assertEquals("idx", mdcAtGetConfiguration.get());
        assertEquals("idx", mdcAtGetState.get());
        assertEquals("idx", mdcAtMetricsSnapshot.get());
        assertEquals("idx", mdcAtPutEntry.get());
        assertEquals("idx", mdcAtGetStream.get());
        assertEquals("idx", mdcAtGetStreamIsolation.get());
        assertEquals("idx", mdcAtGetStreamDefault.get());
        assertEquals("idx", mdcAtGetStreamDefaultIsolation.get());
    }

    @Test
    void propagatesMdcToCompletionStageCallbacks() throws Exception {
        final CompletableFuture<String> delegateStage = new CompletableFuture<>();
        final AtomicReference<String> mdcAtCompletion = new AtomicReference<>();

        when(delegate.getAsync("k")).thenReturn(delegateStage);

        final CompletableFuture<String> wrappedStage = adapter.getAsync("k")
                .thenApply(value -> {
                    mdcAtCompletion.set(MDC.get("index.name"));
                    return value;
                }).toCompletableFuture();

        final ExecutorService completionExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final Future<?> completionFuture = completionExecutor
                    .submit(() -> delegateStage.complete("value"));
            completionFuture.get(5, TimeUnit.SECONDS);
        } finally {
            completionExecutor.shutdownNow();
        }

        assertEquals("value", wrappedStage.get(5, TimeUnit.SECONDS));
        assertEquals("idx", mdcAtCompletion.get());
        assertNull(MDC.get("index.name"));
    }
}
