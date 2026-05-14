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
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexRuntimeOpenContextTest {

    @Test
    void constructorRejectsNullDirectory() {
        final TypeDescriptor<Integer> keyTypeDescriptor = mock(
                TypeDescriptor.class);
        final TypeDescriptor<String> valueTypeDescriptor = mock(
                TypeDescriptor.class);
        final EffectiveIndexConfiguration<Integer, String> conf = mock(
                EffectiveIndexConfiguration.class);
        final ExecutorRegistry executorRegistry = mock(ExecutorRegistry.class);
        final IndexOperationStatsRecorder operationStatsRecorder =
                new IndexOperationStatsRecorder();
        final MaintenanceStatsRecorder maintenanceStatsRecorder =
                new MaintenanceStatsRecorder();
        final SplitStatsRecorder splitStatsRecorder =
                new SplitStatsRecorder();
        final AtomicLong compactRequestHighWaterMark = new AtomicLong();
        final AtomicLong flushRequestHighWaterMark = new AtomicLong();
        final AtomicLong lastAppliedWalLsn = new AtomicLong();
        final Supplier<SegmentIndexState> stateSupplier = stateSupplier();
        final Consumer<RuntimeException> failureHandler = failureHandler();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeOpenContext<>(null,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        executorRegistry, operationStatsRecorder,
                        maintenanceStatsRecorder, splitStatsRecorder,
                        compactRequestHighWaterMark,
                        flushRequestHighWaterMark, lastAppliedWalLsn,
                        stateSupplier, failureHandler));
        assertEquals("Property 'directoryFacade' must not be null.",
                ex.getMessage());
    }

    @Test
    void constructorExposesValidatedAssemblyInputs() {
        final Directory directory = mock(Directory.class);
        final TypeDescriptor<Integer> keyTypeDescriptor = mock(
                TypeDescriptor.class);
        final TypeDescriptor<String> valueTypeDescriptor = mock(
                TypeDescriptor.class);
        final EffectiveIndexConfiguration<Integer, String> conf = mock(
                EffectiveIndexConfiguration.class);
        final ExecutorRegistry executorRegistry = mock(
                ExecutorRegistry.class);
        final IndexOperationStatsRecorder operationStatsRecorder =
                new IndexOperationStatsRecorder();
        final MaintenanceStatsRecorder maintenanceStatsRecorder =
                new MaintenanceStatsRecorder();
        final SplitStatsRecorder splitStatsRecorder =
                new SplitStatsRecorder();
        final AtomicLong compactRequestHighWaterMark = new AtomicLong();
        final AtomicLong flushRequestHighWaterMark = new AtomicLong();
        final AtomicLong lastAppliedWalLsn = new AtomicLong();
        final Supplier<SegmentIndexState> stateSupplier = stateSupplier();
        final Consumer<RuntimeException> failureHandler = failureHandler();

        final SegmentIndexRuntimeOpenContext<Integer, String> request =
                new SegmentIndexRuntimeOpenContext<>(directory,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        executorRegistry, operationStatsRecorder,
                        maintenanceStatsRecorder, splitStatsRecorder,
                        compactRequestHighWaterMark,
                        flushRequestHighWaterMark, lastAppliedWalLsn,
                        stateSupplier, failureHandler);

        assertSame(operationStatsRecorder, request.operationStatsRecorder);
        assertSame(maintenanceStatsRecorder, request.maintenanceStatsRecorder);
        assertSame(splitStatsRecorder, request.splitStatsRecorder);
        assertSame(compactRequestHighWaterMark,
                request.compactRequestHighWaterMark);
        assertSame(flushRequestHighWaterMark,
                request.flushRequestHighWaterMark);
        assertSame(lastAppliedWalLsn, request.lastAppliedWalLsn);
        assertSame(directory, request.directoryFacade);
        assertSame(keyTypeDescriptor, request.keyTypeDescriptor);
        assertSame(valueTypeDescriptor, request.valueTypeDescriptor);
        assertSame(conf, request.conf);
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
