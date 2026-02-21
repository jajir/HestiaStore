package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
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
}
