package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexRuntimeControlPlaneTest {

    private IndexRuntimeControlPlane controlPlane;
    private AtomicReference<Map<RuntimeSettingKey, Integer>> appliedLimits;
    private AtomicInteger splitThresholdRescanCount;
    private SegmentIndexMetricsSnapshot metricsSnapshot;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        appliedLimits = new AtomicReference<>();
        splitThresholdRescanCount = new AtomicInteger(0);
        metricsSnapshot = mock(SegmentIndexMetricsSnapshot.class);
        controlPlane = new IndexRuntimeControlPlane(conf,
                RuntimeTuningState.fromConfiguration(conf),
                () -> SegmentIndexState.READY, () -> metricsSnapshot,
                appliedLimits::set, splitThresholdRescanCount::incrementAndGet);
    }

    @Test
    void validate_rejectsPartitionBufferNotGreaterThanActivePartition() {
        final RuntimeConfigPatch patch = new RuntimeConfigPatch(
                Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                        Integer.valueOf(8),
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                        Integer.valueOf(8)),
                false, null);

        final RuntimePatchValidation validation = controlPlane.configuration()
                .validate(patch);

        assertFalse(validation.isValid());
        assertEquals(1, validation.getIssues().size());
        assertEquals(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                validation.getIssues().get(0).key());
    }

    @Test
    void apply_updatesEffectiveLimitsAndTriggersSplitThresholdRescan() {
        final RuntimeConfigPatch patch = new RuntimeConfigPatch(
                Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(42)),
                false, Long.valueOf(0L));

        final RuntimePatchResult result = controlPlane.configuration()
                .apply(patch);

        assertTrue(result.isApplied());
        assertNotNull(appliedLimits.get());
        assertEquals(Integer.valueOf(42),
                appliedLimits.get().get(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT));
        assertEquals(1, splitThresholdRescanCount.get());
        assertEquals(1L, result.getSnapshot().revision());
    }

    @Test
    void runtimeSnapshot_exposesSuppliedStateAndMetrics() {
        final IndexRuntimeSnapshot snapshot = controlPlane.runtime().snapshot();

        assertEquals("runtime-control-plane-test", snapshot.getIndexName());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
        assertSame(metricsSnapshot, snapshot.getMetrics());
        assertNotNull(snapshot.getCapturedAt());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("runtime-control-plane-test")
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
                .withIndexWorkerThreadCount(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
