package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentBuildStatus;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentTestHelper;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentIdAllocator;
import org.junit.jupiter.api.Test;

class DefaultSegmentMaterializationServiceTest {

    @Test
    void commitMaterializesReadableSegmentFiles() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                directory, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), newConfiguration(),
                stableSegmentMaintenancePool);
        final SegmentIdAllocator segmentIdAllocator = sequenceAllocator(11);
        final DefaultSegmentMaterializationService<Integer, String> service = new DefaultSegmentMaterializationService<>(
                segmentIdAllocator, directory, segmentFactory);

        try {
            final PreparedSegmentHandle<Integer, String> handle = service
                    .openPreparedSegment();
            final SegmentId segmentId = handle.segmentId();
            handle.write(Entry.of(1, "a"));
            handle.write(Entry.of(2, "b"));
            handle.commit();
            handle.close();

            final SegmentBuildResult<Segment<Integer, String>> buildResult = segmentFactory
                    .buildSegment(segmentId);
            final Segment<Integer, String> segment = buildResult.getValue();
            try {
                assertEquals(SegmentBuildStatus.OK, buildResult.getStatus());
                assertEquals(List.of(Entry.of(1, "a"), Entry.of(2, "b")),
                        readEntries(segment));
            } finally {
                SegmentTestHelper.closeAndAwait(segment);
            }
        } finally {
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void discardRemovesPreparedSegmentDirectory() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                directory, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), newConfiguration(),
                stableSegmentMaintenancePool);
        final SegmentIdAllocator segmentIdAllocator = sequenceAllocator(21);
        final DefaultSegmentMaterializationService<Integer, String> service = new DefaultSegmentMaterializationService<>(
                segmentIdAllocator, directory, segmentFactory);

        try {
            final PreparedSegmentHandle<Integer, String> handle = service
                    .openPreparedSegment();
            final SegmentId segmentId = handle.segmentId();
            handle.write(Entry.of(1, "a"));

            handle.discard();
            handle.close();

            assertFalse(directory.isFileExists(segmentId.getName()));
        } finally {
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    private static SegmentIdAllocator sequenceAllocator(final int firstId) {
        final AtomicInteger sequence = new AtomicInteger(firstId);
        return () -> SegmentId.of(sequence.getAndIncrement());
    }

    private static List<Entry<Integer, String>> readEntries(
            final Segment<Integer, String> segment) {
        final List<Entry<Integer, String>> entries = new ArrayList<>();
        try (EntryIterator<Integer, String> iterator = segment
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION)
                .getValue()) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInPartitionBuffer(10)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withMaxNumberOfKeysInSegment(50)//
                .withMaxNumberOfSegmentsInCache(5)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(128)//
                .withBloomFilterProbabilityOfFalsePositive(0.01)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .withNumberOfSegmentMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withIndexBusyBackoffMillis(1)//
                .withIndexBusyTimeoutMillis(1000)//
                .withContextLoggingEnabled(false)//
                .withName("split-materialization-service-test")//
                .build();
    }
}
