package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexControlPlaneContextLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void controlPlaneMethodsRunWithIndexContext() {
        final IndexControlPlane delegate = mock(IndexControlPlane.class);
        final IndexRuntimeView runtimeView = mock(IndexRuntimeView.class);
        final IndexConfigurationManagement configurationManagement = mock(
                IndexConfigurationManagement.class);
        final IndexContextScopeRunner runner = new IndexContextScopeRunner(
                "idx");
        final AtomicReference<String> indexNameMdc = new AtomicReference<>();
        final AtomicReference<String> runtimeMdc = new AtomicReference<>();
        final AtomicReference<String> configurationMdc = new AtomicReference<>();

        when(delegate.indexName()).thenAnswer(invocation -> {
            indexNameMdc.set(MDC.get("index.name"));
            return "idx";
        });
        when(delegate.runtime()).thenAnswer(invocation -> {
            runtimeMdc.set(MDC.get("index.name"));
            return runtimeView;
        });
        when(delegate.configuration()).thenAnswer(invocation -> {
            configurationMdc.set(MDC.get("index.name"));
            return configurationManagement;
        });

        final IndexControlPlaneContextLoggingAdapter adapter =
                new IndexControlPlaneContextLoggingAdapter(delegate, runner);

        assertEquals("idx", adapter.indexName());
        assertInstanceOf(IndexRuntimeViewContextLoggingAdapter.class,
                adapter.runtime());
        assertInstanceOf(IndexConfigurationManagementContextLoggingAdapter.class,
                adapter.configuration());
        assertEquals("idx", indexNameMdc.get());
        assertEquals("idx", runtimeMdc.get());
        assertEquals("idx", configurationMdc.get());
    }
}
