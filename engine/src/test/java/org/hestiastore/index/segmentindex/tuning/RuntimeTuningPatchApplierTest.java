package org.hestiastore.index.segmentindex.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
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
    private AtomicReference<Map<RuntimeSettingKey, Integer>> appliedLimits;
    private AtomicInteger splitThresholdRescanCount;

    @BeforeEach
    void setUp() {
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                .fromConfiguration(buildConf());
        appliedLimits = new AtomicReference<>();
        splitThresholdRescanCount = new AtomicInteger(0);
        applier = new RuntimeTuningPatchApplier(
                new RuntimeTuningPatchValidator(runtimeTuningState),
                runtimeTuningState, appliedLimits::set,
                splitThresholdRescanCount::incrementAndGet);
    }

    @Test
    void applyReturnsValidationFailureWithoutApplyingLimits() {
        final RuntimePatchResult result = applier
                .apply(new RuntimeConfigPatch(
                        Map.of(
                                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                                Integer.valueOf(9),
                                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                                Integer.valueOf(8)),
                        false, null));

        assertFalse(result.isApplied());
        assertEquals(1, result.getValidation().getIssues().size());
        assertEquals(null, appliedLimits.get());
    }

    @Test
    void applySupportsDryRunWithoutMutatingState() {
        final RuntimePatchResult result = applier
                .apply(new RuntimeConfigPatch(
                        Map.of(
                                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                                Integer.valueOf(42)),
                        true, Long.valueOf(0L)));

        assertFalse(result.isApplied());
        assertNotNull(result.getSnapshot());
        assertEquals(0, splitThresholdRescanCount.get());
    }

    @Test
    void applyUpdatesEffectiveLimitsAndNotifiesSplitThresholdChanges() {
        final RuntimePatchResult result = applier
                .apply(new RuntimeConfigPatch(
                        Map.of(
                                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                                Integer.valueOf(42)),
                        false, Long.valueOf(0L)));

        assertTrue(result.isApplied());
        assertEquals(Integer.valueOf(42),
                appliedLimits.get().get(
                        RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD));
        assertEquals(1, splitThresholdRescanCount.get());
        assertEquals(1L, result.getSnapshot().revision());
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
