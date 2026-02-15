package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentLifecycleMaintenanceTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final List<ChunkFilter> FILTERS = List
            .of(new ChunkFilterDoNothing());

    @Test
    void loadSegment_returnsLoadedSegment_whenDirectoryExistsAndGateReady() {
        try (Fixture fixture = new Fixture()) {
            final SegmentId segmentId = SegmentId.of(1);
            fixture.createSegmentDirectory(segmentId);
            fixture.gate.finishFreezeToReady();

            final Segment<Integer, String> loaded = fixture.maintenance
                    .loadSegment(segmentId);

            assertNotNull(loaded);
            assertEquals(segmentId, loaded.getId());
            fixture.maintenance.closeSegmentIfNeeded(loaded);
        }
    }

    @Test
    void loadSegment_throwsIndexException_whenDirectoryMissing() {
        try (Fixture fixture = new Fixture()) {
            fixture.gate.finishFreezeToReady();

            final IndexException ex = assertThrows(IndexException.class,
                    () -> fixture.maintenance.loadSegment(SegmentId.of(10)));
            assertSame(IndexException.class, ex.getClass());
        }
    }

    @Test
    void loadSegment_throwsBusyException_whenRegistryStateIsNotReady() {
        try (Fixture fixture = new Fixture()) {
            final SegmentId segmentId = SegmentId.of(2);
            fixture.createSegmentDirectory(segmentId);

            assertThrows(SegmentBusyException.class,
                    () -> fixture.maintenance.loadSegment(segmentId));
        }
    }

    @Test
    void closeSegmentIfNeeded_retriesWhenBusyAndEventuallyCloses() {
        try (Fixture fixture = new Fixture()) {
            @SuppressWarnings("unchecked")
            final Segment<Integer, String> segment = Mockito.mock(Segment.class);
            when(segment.getState()).thenReturn(SegmentState.READY,
                    SegmentState.READY, SegmentState.CLOSED);
            when(segment.close()).thenReturn(SegmentResult.busy(),
                    SegmentResult.ok());

            fixture.maintenance.closeSegmentIfNeeded(segment);

            verify(segment, times(2)).close();
        }
    }

    @Test
    void closeSegmentIfNeeded_closesLoadedSegmentToClosedState() {
        try (Fixture fixture = new Fixture()) {
            final SegmentId segmentId = SegmentId.of(3);
            fixture.createSegmentDirectory(segmentId);
            fixture.gate.finishFreezeToReady();
            final Segment<Integer, String> loaded = fixture.maintenance
                    .loadSegment(segmentId);

            fixture.maintenance.closeSegmentIfNeeded(loaded);

            assertEquals(SegmentState.CLOSED, loaded.getState());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MemDirectory directory = new MemDirectory();
        private final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        private final ExecutorService maintenanceExecutor = Executors
                .newSingleThreadExecutor();
        private final IndexConfiguration<Integer, String> conf = newConfiguration();

        private final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                asyncDirectory, KEY_DESCRIPTOR, VALUE_DESCRIPTOR, conf,
                maintenanceExecutor);
        private final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                asyncDirectory);
        private final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        private final SegmentLifecycleMaintenance<Integer, String> maintenance = new SegmentLifecycleMaintenance<>(
                segmentFactory, fileSystem, new IndexRetryPolicy(1, 1000),
                new IndexRetryPolicy(1, 1000), gate);

        private void createSegmentDirectory(final SegmentId segmentId) {
            directory.mkdir(segmentId.getName());
        }

        @Override
        public void close() {
            maintenanceExecutor.shutdownNow();
            asyncDirectory.close();
        }
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(10)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withMaxNumberOfKeysInCache(100)//
                .withMaxNumberOfKeysInSegment(50)//
                .withMaxNumberOfSegmentsInCache(5)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(128)//
                .withBloomFilterProbabilityOfFalsePositive(0.01)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withEncodingFilters(FILTERS)//
                .withDecodingFilters(FILTERS)//
                .withSegmentMaintenanceAutoEnabled(false)//
                .withNumberOfCpuThreads(1)//
                .withNumberOfIoThreads(1)//
                .withNumberOfSegmentIndexMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withIndexBusyBackoffMillis(1)//
                .withIndexBusyTimeoutMillis(1000)//
                .withContextLoggingEnabled(false)//
                .withName("segment-lifecycle-maintenance-test")//
                .build();
    }
}
