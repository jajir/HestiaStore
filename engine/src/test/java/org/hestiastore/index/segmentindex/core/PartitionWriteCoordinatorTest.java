package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionWriteCoordinatorTest {

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    private Directory directory;
    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;
    private PartitionRuntime<Integer, String> partitionRuntime;
    private RuntimeTuningState runtimeTuningState;
    private AtomicInteger drainSchedules;
    private PartitionWriteCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        directory = new MemDirectory();
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                new KeyToSegmentMap<>(directory, new TypeDescriptorInteger()));
        partitionRuntime = new PartitionRuntime<>(Integer::compareTo);
        runtimeTuningState = RuntimeTuningState.fromConfiguration(conf);
        drainSchedules = new AtomicInteger(0);
        stubAdmissionRunner();
        coordinator = new PartitionWriteCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                runtimeTuningState, backgroundSplitCoordinator,
                ignoredSegmentId -> drainSchedules.incrementAndGet());
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void putBuffered_createsBootstrapRouteAndSchedulesDrainAfterRotation() {
        runtimeTuningState.apply(Map.of(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                Integer.valueOf(1)));

        final IndexResult<Void> result = coordinator.putBuffered(11, "v11");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertEquals(SegmentId.of(0),
                synchronizedKeyToSegmentMap.findSegmentId(11));
        assertTrue(partitionRuntime.lookup(SegmentId.of(0), 11).isFound());
        assertEquals(1, drainSchedules.get());
    }

    @Test
    void putBuffered_usesEffectiveRuntimeLimitsForBackpressure() {
        runtimeTuningState.apply(Map.of(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                Integer.valueOf(1),
                RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                Integer.valueOf(1),
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                Integer.valueOf(2),
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                Integer.valueOf(2)));

        assertEquals(IndexResultStatus.OK,
                coordinator.putBuffered(1, "v1").getStatus());
        assertEquals(IndexResultStatus.OK,
                coordinator.putBuffered(2, "v2").getStatus());

        final IndexResult<Void> result = coordinator.putBuffered(3, "v3");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void stubAdmissionRunner() {
        doAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get())
                .when(backgroundSplitCoordinator)
                .runWithStableWriteAdmission(any());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("partition-write-coordinator-test")
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
