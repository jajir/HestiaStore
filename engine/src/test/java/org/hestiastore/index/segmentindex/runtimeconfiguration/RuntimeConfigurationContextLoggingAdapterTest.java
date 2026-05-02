package org.hestiastore.index.segmentindex.runtimeconfiguration;

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
class RuntimeConfigurationContextLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void configurationMethodsRunWithIndexContext() {
        final RuntimeConfiguration delegate = mock(
                RuntimeConfiguration.class);
        final RuntimeConfigPatch patch = mock(RuntimeConfigPatch.class);
        final ConfigurationSnapshot actual = mock(ConfigurationSnapshot.class);
        final ConfigurationSnapshot original = mock(ConfigurationSnapshot.class);
        final RuntimePatchValidation validation = mock(
                RuntimePatchValidation.class);
        final RuntimePatchResult result = mock(RuntimePatchResult.class);
        final AtomicReference<String> actualMdc = new AtomicReference<>();
        final AtomicReference<String> originalMdc = new AtomicReference<>();
        final AtomicReference<String> validateMdc = new AtomicReference<>();
        final AtomicReference<String> applyMdc = new AtomicReference<>();

        when(delegate.getCurrent()).thenAnswer(invocation -> {
            actualMdc.set(MDC.get("index.name"));
            return actual;
        });
        when(delegate.getOriginal()).thenAnswer(invocation -> {
            originalMdc.set(MDC.get("index.name"));
            return original;
        });
        when(delegate.validate(patch)).thenAnswer(invocation -> {
            validateMdc.set(MDC.get("index.name"));
            return validation;
        });
        when(delegate.apply(patch)).thenAnswer(invocation -> {
            applyMdc.set(MDC.get("index.name"));
            return result;
        });

        final RuntimeConfigurationContextLoggingAdapter adapter =
                new RuntimeConfigurationContextLoggingAdapter(delegate,
                        new IndexMdcScopeRunner("idx"));

        assertSame(actual, adapter.getCurrent());
        assertSame(original, adapter.getOriginal());
        assertSame(validation, adapter.validate(patch));
        assertSame(result, adapter.apply(patch));
        assertSame("idx", actualMdc.get());
        assertSame("idx", originalMdc.get());
        assertSame("idx", validateMdc.get());
        assertSame("idx", applyMdc.get());
    }
}
