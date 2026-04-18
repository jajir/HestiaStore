package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentBuildStatus;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentTestHelper;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.junit.jupiter.api.Test;

class SegmentFactoryTest {

    @Test
    void buildSegment_createsSegmentWithId() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentFactory<Integer, String> factory = new SegmentFactory<>(
                directory, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf,
                conf.resolveRuntimeConfiguration(),
                stableSegmentMaintenancePool);
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentBuildResult<Segment<Integer, String>> buildResult = factory
                .buildSegment(segmentId);
        final Segment<Integer, String> segment = buildResult.getValue();
        try {
            assertEquals(SegmentBuildStatus.OK, buildResult.getStatus());
            assertNotNull(segment);
            assertEquals(segmentId, segment.getId());
        } finally {
            SegmentTestHelper.closeAndAwait(segment);
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void buildSegment_enablesDirectoryLockingForRegistrySegments() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentFactory<Integer, String> factory = new SegmentFactory<>(
                directory, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf,
                conf.resolveRuntimeConfiguration(),
                stableSegmentMaintenancePool);
        final SegmentId segmentId = SegmentId.of(7);
        final String lockFileName = new SegmentDirectoryLayout(segmentId)
                .getLockFileName();
        final Directory segmentDirectory = directory
                .openSubDirectory(segmentId.getName());

        final SegmentBuildResult<Segment<Integer, String>> buildResult = factory
                .buildSegment(segmentId);
        final Segment<Integer, String> segment = buildResult.getValue();
        try {
            assertEquals(SegmentBuildStatus.OK, buildResult.getStatus());
            assertTrue(segmentDirectory.isFileExists(lockFileName));
        } finally {
            SegmentTestHelper.closeAndAwait(segment);
            stableSegmentMaintenancePool.shutdownNow();
        }
        assertFalse(segmentDirectory.isFileExists(lockFileName));
    }

    @Test
    void openWriterTx_materializesSegmentFilesSynchronously() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentFactory<Integer, String> factory = new SegmentFactory<>(
                directory, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf,
                conf.resolveRuntimeConfiguration(),
                stableSegmentMaintenancePool);
        final SegmentId segmentId = SegmentId.of(9);

        try {
            factory.openWriterTx(segmentId).execute(writer -> {
                writer.write(1, "a");
                writer.write(2, "b");
            });

            final SegmentBuildResult<Segment<Integer, String>> buildResult = factory
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
    void newSegmentBuilderUsesSupplierBackedChunkFiltersFromConfiguration() {
        final AtomicInteger sequence = new AtomicInteger();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(10)
                .withMaxNumberOfKeysInSegmentChunk(4)
                .withMaxNumberOfDeltaCacheFiles(2)
                .withMaxNumberOfKeysInSegment(50)
                .withMaxNumberOfSegmentsInCache(5)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(128)
                .withBloomFilterProbabilityOfFalsePositive(0.01)
                .withDiskIoBufferSizeInBytes(1024)
                .withBackgroundMaintenanceAutoEnabled(false)
                .withNumberOfSegmentMaintenanceThreads(1)
                .withNumberOfIndexMaintenanceThreads(1)
                .withIndexBusyBackoffMillis(1)
                .withIndexBusyTimeoutMillis(1000)
                .withContextLoggingEnabled(false)
                .withName("segment-factory-supplier-test")
                .withEncodingFilterRegistrations(List.of())
                .withDecodingFilterRegistrations(List.of())
                .addEncodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        ChunkFilterSpec.ofProvider("test")
                                .withParameter("keyRef", "orders-main"))
                .addDecodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        ChunkFilterSpec.ofProvider("test")
                                .withParameter("keyRef", "orders-main"))
                .build();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .builder().withDefaultProviders()
                .withProvider(new ChunkFilterProvider() {
                    @Override
                    public String getProviderId() {
                        return "test";
                    }

                    @Override
                    public Supplier<? extends ChunkFilter> createEncodingSupplier(
                            final ChunkFilterSpec spec) {
                        return () -> new TrackingChunkFilter(
                                sequence.incrementAndGet());
                    }

                    @Override
                    public Supplier<? extends ChunkFilter> createDecodingSupplier(
                            final ChunkFilterSpec spec) {
                        return () -> new TrackingChunkFilter(
                                sequence.incrementAndGet());
                    }
                })
                .build();
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration = conf
                .resolveRuntimeConfiguration(registry);
        final SegmentFactory<Integer, String> factory = new SegmentFactory<>(
                new MemDirectory(), new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf, runtimeConfiguration,
                stableSegmentMaintenancePool);

        try {
            final SegmentBuilder<Integer, String> builder = factory
                    .newSegmentBuilder(SegmentId.of(8));
            final List<Supplier<? extends ChunkFilter>> suppliers = readEncodingSuppliers(
                    builder);
            final TrackingChunkFilter first = (TrackingChunkFilter) suppliers
                    .get(0).get();
            final TrackingChunkFilter second = (TrackingChunkFilter) suppliers
                    .get(0).get();

            assertTrue(first.getId() > 0);
            assertEquals(first.getId() + 1, second.getId());
            assertNotSame(first, second);
        } finally {
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void updateRuntimeLimitsRejectsInvalidPartitionBufferRelationship() {
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final SegmentFactory<Integer, String> factory = new SegmentFactory<>(
                new MemDirectory(), new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf,
                conf.resolveRuntimeConfiguration(),
                stableSegmentMaintenancePool);
        try {
            final IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> factory
                            .updateRuntimeLimits(new SegmentRuntimeLimits(10, 5,
                                    5)));
            assertEquals(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache",
                    exception.getMessage());
        } finally {
            stableSegmentMaintenancePool.shutdownNow();
        }
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
                .withName("segment-factory-test")//
                .build();
    }

    @SuppressWarnings("unchecked")
    private static List<Supplier<? extends ChunkFilter>> readEncodingSuppliers(
            final SegmentBuilder<Integer, String> builder) {
        try {
            final Field field = SegmentBuilder.class
                    .getDeclaredField("encodingChunkFilters");
            field.setAccessible(true);
            return (List<Supplier<? extends ChunkFilter>>) field.get(builder);
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
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

    private static final class TrackingChunkFilter implements ChunkFilter {

        private final int id;

        private TrackingChunkFilter(final int id) {
            this.id = id;
        }

        private int getId() {
            return id;
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
