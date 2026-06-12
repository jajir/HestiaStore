package org.hestiastore.index.segmentindex.logging;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningValidation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
class RuntimeTuningMdcLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void configurationMethodsRunWithIndexContext() {
        final RuntimeTuning delegate = mock(
                RuntimeTuning.class);
        final RuntimeTuningPatch patch = mock(RuntimeTuningPatch.class);
        final RuntimeTuningSnapshot actual = mock(RuntimeTuningSnapshot.class);
        final RuntimeTuningSnapshot original = mock(RuntimeTuningSnapshot.class);
        final RuntimeTuningValidation validation = mock(
                RuntimeTuningValidation.class);
        final RuntimeTuningResult result = mock(RuntimeTuningResult.class);
        final AtomicReference<String> actualMdc = new AtomicReference<>();
        final AtomicReference<String> originalMdc = new AtomicReference<>();
        final AtomicReference<String> validateMdc = new AtomicReference<>();
        final AtomicReference<String> applyMdc = new AtomicReference<>();

        when(delegate.current()).thenAnswer(invocation -> {
            actualMdc.set(MDC.get("index.name"));
            return actual;
        });
        when(delegate.original()).thenAnswer(invocation -> {
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

        final RuntimeTuningMdcLoggingAdapter adapter =
                new RuntimeTuningMdcLoggingAdapter(delegate,
                        new IndexMdcCallWrapper("idx"));

        assertSame(actual, adapter.current());
        assertSame(original, adapter.original());
        assertSame(validation, adapter.validate(patch));
        assertSame(result, adapter.apply(patch));
        assertSame("idx", actualMdc.get());
        assertSame("idx", originalMdc.get());
        assertSame("idx", validateMdc.get());
        assertSame("idx", applyMdc.get());
    }
}
