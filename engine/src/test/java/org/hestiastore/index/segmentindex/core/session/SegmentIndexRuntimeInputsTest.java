package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class SegmentIndexRuntimeInputsTest {

    @Test
    void constructorRejectsNullLogger() {
        final Directory directory = mock(Directory.class);
        final TypeDescriptor<Integer> keyTypeDescriptor = mock(
                TypeDescriptor.class);
        final TypeDescriptor<String> valueTypeDescriptor = mock(
                TypeDescriptor.class);
        final IndexConfiguration<Integer, String> conf = mock(
                IndexConfiguration.class);
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                mock(IndexRuntimeConfiguration.class);
        final ExecutorRegistry executorRegistry = mock(ExecutorRegistry.class);
        final Stats stats = new Stats();
        final AtomicLong compactRequestHighWaterMark = new AtomicLong();
        final AtomicLong flushRequestHighWaterMark = new AtomicLong();
        final AtomicLong lastAppliedWalLsn = new AtomicLong();
        final Supplier<SegmentIndexState> stateSupplier = stateSupplier();
        final Consumer<RuntimeException> failureHandler = failureHandler();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeInputs<>(null, directory,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        runtimeConfiguration, executorRegistry, stats,
                        compactRequestHighWaterMark,
                        flushRequestHighWaterMark, lastAppliedWalLsn,
                        stateSupplier, failureHandler));
        assertEquals("Property 'logger' must not be null.", ex.getMessage());
    }

    @Test
    void constructorExposesValidatedAssemblyInputs() {
        final Logger logger = mock(Logger.class);
        final Directory directory = mock(Directory.class);
        final TypeDescriptor<Integer> keyTypeDescriptor = mock(
                TypeDescriptor.class);
        final TypeDescriptor<String> valueTypeDescriptor = mock(
                TypeDescriptor.class);
        final IndexConfiguration<Integer, String> conf = mock(
                IndexConfiguration.class);
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                mock(IndexRuntimeConfiguration.class);
        final ExecutorRegistry executorRegistry = mock(
                ExecutorRegistry.class);
        final Stats stats = new Stats();
        final AtomicLong compactRequestHighWaterMark = new AtomicLong();
        final AtomicLong flushRequestHighWaterMark = new AtomicLong();
        final AtomicLong lastAppliedWalLsn = new AtomicLong();
        final Supplier<SegmentIndexState> stateSupplier = stateSupplier();
        final Consumer<RuntimeException> failureHandler = failureHandler();

        final SegmentIndexRuntimeInputs<Integer, String> request =
                new SegmentIndexRuntimeInputs<>(logger, directory,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        runtimeConfiguration, executorRegistry, stats,
                        compactRequestHighWaterMark,
                        flushRequestHighWaterMark, lastAppliedWalLsn,
                        stateSupplier, failureHandler);

        assertSame(logger, request.logger);
        assertSame(stats, request.stats);
        assertSame(compactRequestHighWaterMark,
                request.compactRequestHighWaterMark);
        assertSame(flushRequestHighWaterMark,
                request.flushRequestHighWaterMark);
        assertSame(lastAppliedWalLsn, request.lastAppliedWalLsn);
        assertSame(directory, request.directoryFacade);
        assertSame(keyTypeDescriptor, request.keyTypeDescriptor);
        assertSame(valueTypeDescriptor, request.valueTypeDescriptor);
        assertSame(conf, request.conf);
        assertSame(runtimeConfiguration, request.runtimeConfiguration);
        assertSame(executorRegistry, request.executorRegistry);
        assertSame(stateSupplier, request.stateSupplier);
        assertSame(failureHandler, request.failureHandler);
    }

    private static Supplier<SegmentIndexState> stateSupplier() {
        return () -> SegmentIndexState.READY;
    }

    private static Consumer<RuntimeException> failureHandler() {
        return failure -> {
        };
    }
}
