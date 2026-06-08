package org.hestiastore.index.segmentregistry;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

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

import org.hestiastore.index.OperationResult;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentLoadCloseOperationsTest {

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

            final Segment<Integer, String> loaded = fixture.segmentOperations
                    .loadSegment(segmentId);

            assertNotNull(loaded);
            assertEquals(segmentId, loaded.getId());
            fixture.segmentOperations.closeSegmentIfNeeded(loaded);
        }
    }

    @Test
    void loadSegment_throwsBusyException_whenDirectoryMissing() {
        try (Fixture fixture = new Fixture()) {
            fixture.gate.finishFreezeToReady();
            final SegmentId missingSegmentId = SegmentId.of(10);
            final SegmentLoadCloseOperations<Integer, String> segmentOperations =
                    fixture.segmentOperations;

            final SegmentBusyException ex = assertThrows(
                    SegmentBusyException.class,
                    () -> segmentOperations.loadSegment(missingSegmentId));
            assertSame(SegmentBusyException.class, ex.getClass());
        }
    }

    @Test
    void loadSegment_throwsBusyException_whenRegistryStateIsNotReady() {
        try (Fixture fixture = new Fixture()) {
            final SegmentId segmentId = SegmentId.of(2);
            fixture.createSegmentDirectory(segmentId);
            final SegmentLoadCloseOperations<Integer, String> segmentOperations =
                    fixture.segmentOperations;

            assertThrows(SegmentBusyException.class,
                    () -> segmentOperations.loadSegment(segmentId));
        }
    }

    @Test
    void closeSegmentIfNeeded_retriesWhenBusyAndEventuallyCloses() {
        try (Fixture fixture = new Fixture()) {
            @SuppressWarnings("unchecked")
            final Segment<Integer, String> segment = Mockito.mock(Segment.class);
            when(segment.getState()).thenReturn(SegmentState.READY,
                    SegmentState.READY, SegmentState.CLOSED);
            when(segment.close()).thenReturn(OperationResult.busy())
                    .thenReturn(OperationResult.ok());

            fixture.segmentOperations.closeSegmentIfNeeded(segment);

            verify(segment, times(2)).close();
        }
    }

    @Test
    void closeSegmentIfNeeded_closesLoadedSegmentToClosedState() {
        try (Fixture fixture = new Fixture()) {
            final SegmentId segmentId = SegmentId.of(3);
            fixture.createSegmentDirectory(segmentId);
            fixture.gate.finishFreezeToReady();
            final Segment<Integer, String> loaded = fixture.segmentOperations
                    .loadSegment(segmentId);

            fixture.segmentOperations.closeSegmentIfNeeded(loaded);

            assertEquals(SegmentState.CLOSED, loaded.getState());
        }
    }

    @Test
    void loadSegment_blocks_reopen_when_segment_directory_locking_is_enabled() {
        try (Fixture fixture = new Fixture()) {
            final SegmentId segmentId = SegmentId.of(4);
            fixture.createSegmentDirectory(segmentId);
            fixture.gate.finishFreezeToReady();
            final Segment<Integer, String> loaded = fixture.segmentOperations
                    .loadSegment(segmentId);
            final SegmentLoadCloseOperations<Integer, String> segmentOperations =
                    fixture.segmentOperations;
            try {
                assertThrows(SegmentBusyException.class,
                        () -> segmentOperations.loadSegment(segmentId));
            } finally {
                fixture.segmentOperations.closeSegmentIfNeeded(loaded);
            }
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MemDirectory directory = new MemDirectory();
        private final Directory asyncDirectory = directory;
        private final ExecutorService stableSegmentMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        private final IndexConfiguration<Integer, String> conf = newConfiguration();

        private final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                asyncDirectory, KEY_DESCRIPTOR, VALUE_DESCRIPTOR,
                effective(conf),
                stableSegmentMaintenanceExecutor);
        private final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                asyncDirectory);
        private final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        private final SegmentLoadCloseOperations<Integer, String> segmentOperations = new SegmentLoadCloseOperations<>(
                segmentFactory, fileSystem,
                new RegistryMaintenanceRetryPolicy(1, 1000),
                gate);

        private void createSegmentDirectory(final SegmentId segmentId) {
            directory.mkdir(segmentId.getName());
        }

        @Override
        public void close() {
            stableSegmentMaintenanceExecutor.shutdownNow();
        }
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class)//
                        .valueClass(String.class)//
                        .keyTypeDescriptor(KEY_DESCRIPTOR)//
                        .valueTypeDescriptor(VALUE_DESCRIPTOR)//
                        .name("segment-load-close-operations-test"))//
                .segment(segment -> segment.cacheKeyLimit(10))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(10))//
                .segment(segment -> segment.chunkKeyLimit(4)//
                        .deltaCacheFileLimit(2)//
                        .maxKeys(50)//
                        .cachedSegmentLimit(5))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1)//
                        .indexSizeBytes(128)//
                        .falsePositiveProbability(0.01))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .filters(filters -> filters.encodingFilters(FILTERS)//
                        .decodingFilters(FILTERS))//
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false)//
                        .segmentThreads(1)//
                        .indexThreads(1)//
                        .busyBackoffMillis(1)//
                        .busyTimeoutMillis(1000))//
                .logging(logging -> logging.contextEnabled(false))//
                .build();
    }
}
