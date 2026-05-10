package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeTuningPatchApplierTest {

    private RuntimeTuningPatchApplier applier;
    private RuntimeTuningState runtimeTuningState;
    private AtomicReference<RuntimeTuningSnapshot> appliedLimits;
    private AtomicInteger splitThresholdRescanCount;

    @BeforeEach
    void setUp() {
        runtimeTuningState = RuntimeTuningState.fromConfiguration(
                effective(buildConf()));
        appliedLimits = new AtomicReference<>();
        splitThresholdRescanCount = new AtomicInteger(0);
        applier = new RuntimeTuningPatchApplier(
                new RuntimeTuningPatchValidator(runtimeTuningState),
                runtimeTuningState, appliedLimits::set,
                splitThresholdRescanCount::incrementAndGet);
    }

    @Test
    void applyReturnsValidationFailureWithoutApplyingLimits() {
        final RuntimeTuningResult result = applier.apply(RuntimeTuningPatch
                .builder()
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(9)
                        .segmentWriteCacheKeyLimitDuringMaintenance(8))
                .build());

        assertFalse(result.applied());
        assertEquals(RuntimeTuningApplyStatus.REJECTED, result.status());
        assertEquals(1, result.validation().issues().size());
        assertNull(appliedLimits.get());
        assertTrue(result.changes().isEmpty());
    }

    @Test
    void validateIsTheDryRunPathAndDoesNotMutateState() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(42))
                .build();

        final RuntimeTuningValidation validation =
                new RuntimeTuningPatchValidator(runtimeTuningState)
                        .validate(patch);

        assertTrue(validation.valid());
        assertEquals(0L, runtimeTuningState.revision());
        assertEquals(0, splitThresholdRescanCount.get());
        assertNull(appliedLimits.get());
    }

    @Test
    void applyUpdatesEffectiveLimitsAndNotifiesSplitThresholdChanges() {
        final RuntimeTuningResult result = applier.apply(RuntimeTuningPatch
                .builder()
                .expectedRevision(0L)
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(42))
                .build());

        assertTrue(result.applied());
        assertNotNull(appliedLimits.get());
        assertEquals(42, appliedLimits.get().writePath()
                .segmentSplitKeyThreshold());
        assertEquals(1, splitThresholdRescanCount.get());
        assertEquals(1L, result.after().revision());
    }

    @Test
    void sideEffectFailureDoesNotMutateRuntimeState() {
        final RuntimeTuningPatchApplier failingApplier =
                new RuntimeTuningPatchApplier(
                        new RuntimeTuningPatchValidator(runtimeTuningState),
                        runtimeTuningState, ignored -> {
                            throw new IllegalStateException("failed");
                        }, splitThresholdRescanCount::incrementAndGet);

        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(42))
                .build();

        assertThrows(IllegalStateException.class,
                () -> failingApplier.apply(patch));
        assertEquals(0L, runtimeTuningState.revision());
        assertEquals(50, runtimeTuningState.segmentSplitKeyThreshold());
        assertEquals(0, splitThresholdRescanCount.get());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-config-patch-applier-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(7))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(50))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
