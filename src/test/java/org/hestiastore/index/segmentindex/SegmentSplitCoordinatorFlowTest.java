package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
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
        final TrackingRegistry registry = new TrackingRegistry(directory, conf,
                true);
        final KeyToSegmentMap<Integer> rawKeyMap = newKeyMap(List.of(
                Entry.of(100, SegmentId.of(0))));
        final KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                rawKeyMap);
        final SegmentSplitCoordinator<Integer, String> coordinator = new SegmentSplitCoordinator<>(
                conf, keyToSegmentMap, registry);
        try {
            final Segment<Integer, String> segment = registry
                    .getSegment(SegmentId.of(0)).getValue();
            registry.clearCreatedSegments();
            for (int i = 0; i < 4; i++) {
                assertEquals(SegmentResultStatus.OK,
                        segment.put(i, "v-" + i).getStatus());
            }

            coordinator.optionallySplit(segment, 2);

            final List<SegmentId> created = registry.getCreatedSegments();
            assertEquals(2, created.size());
            assertTrue(registry.getDeletedSegments().containsAll(created));
            assertFalse(registry.getDeletedSegments()
                    .contains(SegmentId.of(0)));
            assertFalse(registry.wasSwapCalled());
        } finally {
            keyToSegmentMap.close();
            registry.close();
        }
    }

    @Test
    void split_doesNotSwapDirectoriesOnSuccess() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final AsyncDirectory directory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final TrackingRegistry registry = new TrackingRegistry(directory, conf,
                false);
        final KeyToSegmentMap<Integer> rawKeyMap = newKeyMap(List.of(
                Entry.of(100, SegmentId.of(0))));
        final KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                rawKeyMap);
        final SegmentSplitCoordinator<Integer, String> coordinator = new SegmentSplitCoordinator<>(
                conf, keyToSegmentMap, registry);
        try {
            final Segment<Integer, String> segment = registry
                    .getSegment(SegmentId.of(0)).getValue();
            registry.clearCreatedSegments();
            for (int i = 0; i < 4; i++) {
                assertEquals(SegmentResultStatus.OK,
                        segment.put(i, "v-" + i).getStatus());
            }

            coordinator.optionallySplit(segment, 2);

            assertFalse(registry.wasSwapCalled());
            assertEquals(2, keyToSegmentMap.getSegmentIds().size());
        } finally {
            keyToSegmentMap.close();
            registry.close();
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
        private final List<SegmentId> createdSegments = new ArrayList<>();
        private final boolean forceApplyFailure;
        private boolean swapCalled;

        private TrackingRegistry(final AsyncDirectory directoryFacade,
                final IndexConfiguration<Integer, String> conf,
                final boolean forceApplyFailure) {
            super(directoryFacade, KEY_DESCRIPTOR, VALUE_DESCRIPTOR, conf);
            this.forceApplyFailure = forceApplyFailure;
        }

        @Override
        public SegmentBuilder<Integer, String> newSegmentBuilder(
                final SegmentId segmentId) {
            createdSegments.add(segmentId);
            return super.newSegmentBuilder(segmentId);
        }

        @Override
        public SegmentRegistryResult<Segment<Integer, String>> applySplitPlan(
                final SegmentSplitApplyPlan<Integer, String> plan,
                final Segment<Integer, String> lowerSegment,
                final Segment<Integer, String> upperSegment,
                final java.util.function.BooleanSupplier onApplied) {
            if (forceApplyFailure) {
                return SegmentRegistryResult.busy();
            }
            return super.applySplitPlan(plan, lowerSegment, upperSegment,
                    onApplied);
        }

        @Override
        public void deleteSegmentFiles(final SegmentId segmentId) {
            deletedSegments.add(segmentId);
            super.deleteSegmentFiles(segmentId);
        }

        @Override
        public void swapSegmentDirectories(final SegmentId segmentId,
                final SegmentId replacementSegmentId) {
            swapCalled = true;
            super.swapSegmentDirectories(segmentId, replacementSegmentId);
        }

        private List<SegmentId> getDeletedSegments() {
            return deletedSegments;
        }

        private List<SegmentId> getCreatedSegments() {
            return createdSegments;
        }

        private void clearCreatedSegments() {
            createdSegments.clear();
        }

        private boolean wasSwapCalled() {
            return swapCalled;
        }
    }
}
