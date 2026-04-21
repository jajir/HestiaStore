package org.hestiastore.index.segmentindex.core.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeConfigPatchApplierTest {

    private RuntimeConfigPatchApplier applier;
    private AtomicReference<Map<RuntimeSettingKey, Integer>> appliedLimits;
    private AtomicInteger splitThresholdRescanCount;

    @BeforeEach
    void setUp() {
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                .fromConfiguration(buildConf());
        appliedLimits = new AtomicReference<>();
        splitThresholdRescanCount = new AtomicInteger(0);
        applier = new RuntimeConfigPatchApplier(
                new RuntimeConfigPatchValidator(runtimeTuningState),
                runtimeTuningState, appliedLimits::set,
                splitThresholdRescanCount::incrementAndGet);
    }

    @Test
    void applyReturnsValidationFailureWithoutApplyingLimits() {
        final RuntimePatchResult result = applier
                .apply(new RuntimeConfigPatch(
                        Map.of(
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                                Integer.valueOf(9),
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
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
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
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
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                                Integer.valueOf(42)),
                        false, Long.valueOf(0L)));

        assertTrue(result.isApplied());
        assertEquals(Integer.valueOf(42),
                appliedLimits.get().get(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT));
        assertEquals(1, splitThresholdRescanCount.get());
        assertEquals(1L, result.getSnapshot().revision());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("runtime-config-patch-applier-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfImmutableRunsPerPartition(2)
                .withMaxNumberOfKeysInPartitionBuffer(7)
                .withMaxNumberOfKeysInIndexBuffer(9)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfKeysInPartitionBeforeSplit(50)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
