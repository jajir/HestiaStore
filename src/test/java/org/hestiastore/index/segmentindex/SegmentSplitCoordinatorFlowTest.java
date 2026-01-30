package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.Entry;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentIdAllocator;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryGate;
import org.hestiastore.index.segmentregistry.SegmentRegistryState;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.Test;

class SegmentSplitCoordinatorFlowTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @Test
    void split_applyFailureDeletesNewSegments() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final AsyncDirectory directory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                newKeyMap(List.of(Entry.of(100, SegmentId.of(0)))));
        final SegmentAsyncExecutor maintenanceExecutor = new SegmentAsyncExecutor(
                1, "segment-maintenance");
        final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                directory, KEY_DESCRIPTOR, VALUE_DESCRIPTOR, conf,
                maintenanceExecutor.getExecutor());
        final AtomicInteger nextId = new AtomicInteger(1);
        final SegmentIdAllocator segmentIdAllocator = () -> SegmentId
                .of(nextId.getAndIncrement());
        final TrackingRegistry registry = new TrackingRegistry(directory,
                segmentFactory, segmentIdAllocator, conf);
        final TrackingWriterTxFactory writerTxFactory = new TrackingWriterTxFactory(
                segmentFactory);
        final SegmentRegistryAccess<Integer, String> registryAccess = new SegmentRegistryAccessAdapter<>(
                registry);
        final SegmentSplitCoordinator<Integer, String> coordinator = new SegmentSplitCoordinator<>(
                conf, keyToSegmentMap, registry, registryAccess,
                writerTxFactory);
        try {
            final Segment<Integer, String> segment = registry
                    .getSegment(SegmentId.of(0)).getValue();
            writerTxFactory.clearCreatedSegments();
            for (int i = 0; i < 4; i++) {
                assertEquals(SegmentResultStatus.OK,
                        segment.put(i, "v-" + i).getStatus());
            }

            keyToSegmentMap.removeSegment(SegmentId.of(0));
            coordinator.optionallySplit(segment, 2);

            final List<SegmentId> created = writerTxFactory.getCreatedSegments();
            assertEquals(2, created.size());
            assertTrue(registry.getDeletedSegments().containsAll(created));
            assertFalse(registry.getDeletedSegments()
                    .contains(SegmentId.of(0)));
            assertEquals(SegmentRegistryState.ERROR,
                    readGate(registry).getState());
        } finally {
            keyToSegmentMap.close();
            registry.close();
            if (!maintenanceExecutor.wasClosed()) {
                maintenanceExecutor.close();
            }
        }
    }

    private static SegmentRegistryGate readGate(
            final SegmentRegistryImpl<?, ?> registry) {
        try {
            final var field = SegmentRegistryImpl.class
                    .getDeclaredField("gate");
            field.setAccessible(true);
            return (SegmentRegistryGate) field.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read registry gate", ex);
        }
    }

    @Test
    void split_doesNotSwapDirectoriesOnSuccess() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final AsyncDirectory directory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                newKeyMap(List.of(Entry.of(100, SegmentId.of(0)))));
        final SegmentAsyncExecutor maintenanceExecutor = new SegmentAsyncExecutor(
                1, "segment-maintenance");
        final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                directory, KEY_DESCRIPTOR, VALUE_DESCRIPTOR, conf,
                maintenanceExecutor.getExecutor());
        final AtomicInteger nextId = new AtomicInteger(1);
        final SegmentIdAllocator segmentIdAllocator = () -> SegmentId
                .of(nextId.getAndIncrement());
        final TrackingRegistry registry = new TrackingRegistry(directory,
                segmentFactory, segmentIdAllocator, conf);
        final TrackingWriterTxFactory writerTxFactory = new TrackingWriterTxFactory(
                segmentFactory);
        final SegmentRegistryAccess<Integer, String> registryAccess = new SegmentRegistryAccessAdapter<>(
                registry);
        final SegmentSplitCoordinator<Integer, String> coordinator = new SegmentSplitCoordinator<>(
                conf, keyToSegmentMap, registry, registryAccess,
                writerTxFactory);
        try {
            final Segment<Integer, String> segment = registry
                    .getSegment(SegmentId.of(0)).getValue();
            writerTxFactory.clearCreatedSegments();
            for (int i = 0; i < 4; i++) {
                assertEquals(SegmentResultStatus.OK,
                        segment.put(i, "v-" + i).getStatus());
            }

            coordinator.optionallySplit(segment, 2);

            assertEquals(2, keyToSegmentMap.getSegmentIds().size());
        } finally {
            keyToSegmentMap.close();
            registry.close();
            if (!maintenanceExecutor.wasClosed()) {
                maintenanceExecutor.close();
            }
        }
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        final IndexConfigurationContract defaults = new IndexConfigurationDefaultInteger();
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(100)//
                .withMaxNumberOfKeysInSegmentWriteCache(100)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(150)//
                .withMaxNumberOfKeysInSegmentChunk(10)//
                .withMaxNumberOfDeltaCacheFiles(10)//
                .withMaxNumberOfKeysInCache(200)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(5)//
                .withBloomFilterNumberOfHashFunctions(
                        defaults.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        defaults.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        defaults.getBloomFilterProbabilityOfFalsePositive())//
                .withDiskIoBufferSizeInBytes(
                        defaults.getDiskIoBufferSizeInBytes())//
                .withEncodingFilters(defaults.getEncodingChunkFilters())//
                .withDecodingFilters(defaults.getDecodingChunkFilters())//
                .withSegmentMaintenanceAutoEnabled(false)//
                .withNumberOfCpuThreads(1)//
                .withNumberOfIoThreads(1)//
                .withNumberOfSegmentIndexMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withIndexBusyBackoffMillis(1)//
                .withIndexBusyTimeoutMillis(200)//
                .withName("split-flow-test")//
                .build();
    }

    private static KeyToSegmentMap<Integer> newKeyMap(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final SortedDataFile<Integer, SegmentId> sdf = SortedDataFile
                .<Integer, SegmentId>builder()
                .withAsyncDirectory(AsyncDirectoryAdapter.wrap(dir))
                .withFileName("index.map")
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorSegmentId())
                .build();
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new KeyToSegmentMap<>(AsyncDirectoryAdapter.wrap(dir),
                new TypeDescriptorInteger());
    }

    private static final class TrackingRegistry
            extends SegmentRegistryImpl<Integer, String> {
        private final List<SegmentId> deletedSegments = new ArrayList<>();

        private TrackingRegistry(final AsyncDirectory directoryFacade,
                final SegmentFactory<Integer, String> segmentFactory,
                final SegmentIdAllocator segmentIdAllocator,
                final IndexConfiguration<Integer, String> conf) {
            super(directoryFacade, segmentFactory, segmentIdAllocator, conf);
        }

        @Override
        public SegmentRegistryResult<Void> deleteSegment(
                final SegmentId segmentId) {
            deletedSegments.add(segmentId);
            return super.deleteSegment(segmentId);
        }

        private List<SegmentId> getDeletedSegments() {
            return deletedSegments;
        }
    }

    private static final class TrackingWriterTxFactory
            implements SegmentWriterTxFactory<Integer, String> {
        private final SegmentFactory<Integer, String> segmentFactory;
        private final List<SegmentId> createdSegments = new ArrayList<>();

        private TrackingWriterTxFactory(
                final SegmentFactory<Integer, String> segmentFactory) {
            this.segmentFactory = segmentFactory;
        }

        @Override
        public WriteTransaction<Integer, String> openWriterTx(
                final SegmentId segmentId) {
            createdSegments.add(segmentId);
            return segmentFactory.newSegmentBuilder(segmentId).openWriterTx();
        }

        private List<SegmentId> getCreatedSegments() {
            return createdSegments;
        }

        private void clearCreatedSegments() {
            createdSegments.clear();
        }
    }
}
