package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class DefaultSegmentMaterializationServiceTest {

    @Test
    void materializeRouteSplitCreatesReadableChildSegments() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenancePool = Executors
                .newSingleThreadExecutor();
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final SegmentRegistry<Integer, String> registry = openRegistry(
                directory, conf, stableSegmentMaintenancePool,
                registryMaintenancePool);
        final DefaultSegmentMaterializationService<Integer, String> service = new DefaultSegmentMaterializationService<>(
                directory, registry.materialization());

        try {
            final RouteSplitPlan<Integer> splitPlan = service
                    .materializeRouteSplit(
                            registry.loadSegment(openSourceSegment(registry))
                                    .getSegment(),
                            1, 2, Integer::compare,
                            EntryIterator.make(entries().iterator()));

            try {
                final Segment<Integer, String> lowerSegment = registry
                        .loadSegment(splitPlan.getLowerSegmentId())
                        .getSegment();
                final Segment<Integer, String> upperSegment = registry
                        .loadSegment(splitPlan.getUpperSegmentId().orElseThrow())
                        .getSegment();
                assertEquals(List.of(Entry.of(1, "a"), Entry.of(2, "b")),
                        readEntries(lowerSegment));
                assertEquals(List.of(Entry.of(3, "c"), Entry.of(4, "d")),
                        readEntries(upperSegment));
            } finally {
                registry.close();
            }
        } finally {
            registryMaintenancePool.shutdownNow();
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void deletePreparedSegmentRemovesMaterializedSegmentDirectory() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenancePool = Executors
                .newSingleThreadExecutor();
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final SegmentRegistry<Integer, String> registry = openRegistry(
                directory, conf, stableSegmentMaintenancePool,
                registryMaintenancePool);
        final DefaultSegmentMaterializationService<Integer, String> service = new DefaultSegmentMaterializationService<>(
                directory, registry.materialization());

        try {
            final RouteSplitPlan<Integer> splitPlan = service
                    .materializeRouteSplit(
                            registry.loadSegment(openSourceSegment(registry))
                                    .getSegment(),
                            1, 2, Integer::compare,
                            EntryIterator.make(entries().iterator()));
            final SegmentId lowerSegmentId = splitPlan.getLowerSegmentId();
            final SegmentId upperSegmentId = splitPlan.getUpperSegmentId()
                    .orElseThrow();

            assertTrue(directory.isFileExists(lowerSegmentId.getName()));
            assertTrue(directory.isFileExists(upperSegmentId.getName()));

            service.deletePreparedSegment(lowerSegmentId);
            service.deletePreparedSegment(upperSegmentId);

            assertFalse(directory.isFileExists(lowerSegmentId.getName()));
            assertFalse(directory.isFileExists(upperSegmentId.getName()));
        } finally {
            registry.close();
            registryMaintenancePool.shutdownNow();
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    private static SegmentId openSourceSegment(
            final SegmentRegistry<Integer, String> registry) {
        final SegmentId segmentId = registry.materialization().nextSegmentId();
        final var writerTx = registry.materialization().openWriterTx(segmentId);
        try (var writer = writerTx.open()) {
            entries().forEach(writer::write);
        }
        writerTx.commit();
        return segmentId;
    }

    private static List<Entry<Integer, String>> entries() {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"));
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

    private static SegmentRegistry<Integer, String> openRegistry(
            final Directory directory,
            final IndexConfiguration<Integer, String> conf,
            final ExecutorService stableSegmentMaintenancePool,
            final ExecutorService registryMaintenancePool) {
        return SegmentRegistry
                .<Integer, String>builder()
                .withDirectoryFacade(directory)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withConfiguration(conf)
                .withRuntimeConfiguration(conf.resolveRuntimeConfiguration())
                .withSegmentMaintenanceExecutor(stableSegmentMaintenancePool)
                .withRegistryMaintenanceExecutor(registryMaintenancePool)
                .build();
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
