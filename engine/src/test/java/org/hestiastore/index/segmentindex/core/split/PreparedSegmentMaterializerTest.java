package org.hestiastore.index.segmentindex.core.split;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class PreparedSegmentMaterializerTest {

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
        final PreparedSegmentMaterializer<Integer, String> service = new PreparedSegmentMaterializer<>(
                directory, registry.materialization());

        try {
            final RouteSplitPreparation<Integer> prepared = service
                    .materializeRouteSplit(
                            registry.loadSegment(openSourceSegment(registry))
                                    .getSegment(),
                            3L, 3L,
                            EntryIterator.make(entries(6).iterator()));
            final RouteSplitPlan<Integer> splitPlan = prepared.routeSplit()
                    .orElseThrow();

            assertEquals(RouteSplitPreparationStatus.PREPARED,
                    prepared.status());
            assertEquals(Optional.of(6), splitPlan.getUpperMaxKey());
            try {
                final Segment<Integer, String> lowerSegment = registry
                        .loadSegment(splitPlan.getLowerSegmentId())
                        .getSegment();
                final Segment<Integer, String> upperSegment = registry
                        .loadSegment(splitPlan.getUpperSegmentId())
                        .getSegment();
                assertEquals(List.of(Entry.of(1, "a"), Entry.of(2, "b"),
                        Entry.of(3, "c")),
                        readEntries(lowerSegment));
                assertEquals(List.of(Entry.of(4, "d"), Entry.of(5, "e"),
                        Entry.of(6, "f")),
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
    void materializeRouteSplitCreatesReadableOddChildSegments() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenancePool = Executors
                .newSingleThreadExecutor();
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final SegmentRegistry<Integer, String> registry = openRegistry(
                directory, conf, stableSegmentMaintenancePool,
                registryMaintenancePool);
        final PreparedSegmentMaterializer<Integer, String> service = new PreparedSegmentMaterializer<>(
                directory, registry.materialization());

        try {
            final RouteSplitPreparation<Integer> prepared = service
                    .materializeRouteSplit(
                            registry.loadSegment(openSourceSegment(registry,
                                    entries(7))).getSegment(),
                            3L, 3L,
                            EntryIterator.make(entries(7).iterator()));
            final RouteSplitPlan<Integer> splitPlan = prepared.routeSplit()
                    .orElseThrow();

            assertEquals(RouteSplitPreparationStatus.PREPARED,
                    prepared.status());
            assertEquals(Optional.of(7), splitPlan.getUpperMaxKey());
            try {
                final Segment<Integer, String> lowerSegment = registry
                        .loadSegment(splitPlan.getLowerSegmentId())
                        .getSegment();
                final Segment<Integer, String> upperSegment = registry
                        .loadSegment(splitPlan.getUpperSegmentId())
                        .getSegment();
                assertEquals(List.of(Entry.of(1, "a"), Entry.of(2, "b"),
                        Entry.of(3, "c")),
                        readEntries(lowerSegment));
                assertEquals(List.of(Entry.of(4, "d"), Entry.of(5, "e"),
                        Entry.of(6, "f"), Entry.of(7, "g")),
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
        final PreparedSegmentMaterializer<Integer, String> service = new PreparedSegmentMaterializer<>(
                directory, registry.materialization());

        try {
            final RouteSplitPlan<Integer> splitPlan = service
                    .materializeRouteSplit(
                            registry.loadSegment(openSourceSegment(registry))
                                    .getSegment(),
                            3L, 3L,
                            EntryIterator.make(entries(6).iterator()))
                    .routeSplit().orElseThrow();
            final SegmentId lowerSegmentId = splitPlan.getLowerSegmentId();
            final SegmentId upperSegmentId = splitPlan.getUpperSegmentId();

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

    @Test
    void materializeRouteSplitCompactsParentWhenEndOfFileArrivesBeforeTarget() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenancePool = Executors
                .newSingleThreadExecutor();
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final SegmentRegistry<Integer, String> registry = openRegistry(
                directory, conf, stableSegmentMaintenancePool,
                registryMaintenancePool);
        final PreparedSegmentMaterializer<Integer, String> service = new PreparedSegmentMaterializer<>(
                directory, registry.materialization());

        try {
            final SegmentId sourceSegmentId = openSourceSegment(registry,
                    entries(5));
            final RouteSplitPreparation<Integer> prepared = service
                    .materializeRouteSplit(
                            registry.loadSegment(sourceSegmentId).getSegment(),
                            6L, 3L,
                            EntryIterator.make(entries(5).iterator()));

            assertEquals(RouteSplitPreparationStatus.COMPACT_PARENT,
                    prepared.status());
            assertPreparedChildrenDeleted(directory, sourceSegmentId);
        } finally {
            registry.close();
            registryMaintenancePool.shutdownNow();
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void materializeRouteSplitCompactsParentWhenUpperChildIsTooSmall() {
        final Directory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenancePool = Executors
                .newSingleThreadExecutor();
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final SegmentRegistry<Integer, String> registry = openRegistry(
                directory, conf, stableSegmentMaintenancePool,
                registryMaintenancePool);
        final PreparedSegmentMaterializer<Integer, String> service = new PreparedSegmentMaterializer<>(
                directory, registry.materialization());

        try {
            final SegmentId sourceSegmentId = openSourceSegment(registry,
                    entries(5));
            final RouteSplitPreparation<Integer> prepared = service
                    .materializeRouteSplit(
                            registry.loadSegment(sourceSegmentId).getSegment(),
                            3L, 3L,
                            EntryIterator.make(entries(5).iterator()));

            assertEquals(RouteSplitPreparationStatus.COMPACT_PARENT,
                    prepared.status());
            assertPreparedChildrenDeleted(directory, sourceSegmentId);
        } finally {
            registry.close();
            registryMaintenancePool.shutdownNow();
            stableSegmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void materializeRouteSplitDeletesPreparedSegmentsWhenWriteFails() {
        final Directory directory = new MemDirectory();
        @SuppressWarnings("unchecked")
        final SegmentRegistry.Materialization<Integer, String> materialization =
                mock(SegmentRegistry.Materialization.class);
        final SegmentFullWriterTx<Integer, String> lowerTx = writerTx();
        final EntryWriter<Integer, String> lowerWriter = writer();
        when(materialization.nextSegmentId())
                .thenReturn(SegmentId.of(2), SegmentId.of(3));
        when(materialization.openWriterTx(SegmentId.of(2)))
                .thenReturn(lowerTx);
        when(lowerTx.open()).thenReturn(lowerWriter);
        doThrow(new IllegalStateException("write failed"))
                .when(lowerWriter).write(any());
        final PreparedSegmentMaterializer<Integer, String> service =
                new PreparedSegmentMaterializer<>(directory,
                        materialization);

        assertThrows(IllegalStateException.class,
                () -> service.materializeRouteSplit(mock(Segment.class),
                        3L, 3L, EntryIterator.make(entries(6).iterator())));

        assertFalse(directory.isFileExists(SegmentId.of(2).getName()));
        verify(lowerWriter).close();
    }

    @Test
    void materializeRouteSplitDeletesPreparedSegmentsWhenWriterOpenFails() {
        final Directory directory = new MemDirectory();
        @SuppressWarnings("unchecked")
        final SegmentRegistry.Materialization<Integer, String> materialization =
                mock(SegmentRegistry.Materialization.class);
        final SegmentFullWriterTx<Integer, String> lowerTx = writerTx();
        when(materialization.nextSegmentId()).thenReturn(SegmentId.of(2));
        when(materialization.openWriterTx(SegmentId.of(2)))
                .thenReturn(lowerTx);
        when(lowerTx.open()).thenThrow(new IllegalStateException("open failed"));
        final PreparedSegmentMaterializer<Integer, String> service =
                new PreparedSegmentMaterializer<>(directory,
                        materialization);

        assertThrows(IllegalStateException.class,
                () -> service.materializeRouteSplit(mock(Segment.class),
                        3L, 3L, EntryIterator.make(entries(6).iterator())));

        assertFalse(directory.isFileExists(SegmentId.of(2).getName()));
    }

    @Test
    void materializeRouteSplitDeletesPreparedSegmentsWhenCommitFails() {
        final Directory directory = new MemDirectory();
        @SuppressWarnings("unchecked")
        final SegmentRegistry.Materialization<Integer, String> materialization =
                mock(SegmentRegistry.Materialization.class);
        final SegmentFullWriterTx<Integer, String> lowerTx = writerTx();
        final SegmentFullWriterTx<Integer, String> upperTx = writerTx();
        final EntryWriter<Integer, String> lowerWriter = writer();
        final EntryWriter<Integer, String> upperWriter = writer();
        when(materialization.nextSegmentId())
                .thenReturn(SegmentId.of(2), SegmentId.of(3));
        when(materialization.openWriterTx(SegmentId.of(2)))
                .thenReturn(lowerTx);
        when(materialization.openWriterTx(SegmentId.of(3)))
                .thenReturn(upperTx);
        when(lowerTx.open()).thenReturn(lowerWriter);
        when(upperTx.open()).thenReturn(upperWriter);
        doThrow(new IllegalStateException("commit failed"))
                .when(upperTx).commit();
        final PreparedSegmentMaterializer<Integer, String> service =
                new PreparedSegmentMaterializer<>(directory,
                        materialization);

        assertThrows(IllegalStateException.class,
                () -> service.materializeRouteSplit(mock(Segment.class),
                        3L, 3L, EntryIterator.make(entries(6).iterator())));

        assertFalse(directory.isFileExists(SegmentId.of(2).getName()));
        assertFalse(directory.isFileExists(SegmentId.of(3).getName()));
        verify(lowerWriter, atLeastOnce()).close();
        verify(upperWriter, atLeastOnce()).close();
    }

    @Test
    void deletePreparedSegmentFailsWhenDirectoryRemainsOnDisk() {
        final Directory directory = mock(Directory.class);
        final Directory segmentDirectory = mock(Directory.class);
        @SuppressWarnings("unchecked")
        final SegmentRegistry.Materialization<Integer, String> materialization =
                mock(SegmentRegistry.Materialization.class);
        final SegmentId segmentId = SegmentId.of(17);
        when(directory.isFileExists(segmentId.getName()))
                .thenReturn(true, true);
        when(directory.openSubDirectory(segmentId.getName()))
                .thenReturn(segmentDirectory);
        when(segmentDirectory.getFileNames()).thenReturn(Stream.empty());
        when(directory.rmdir(segmentId.getName()))
                .thenThrow(new IllegalStateException("root busy"));
        final PreparedSegmentMaterializer<Integer, String> service =
                new PreparedSegmentMaterializer<>(directory,
                        materialization);

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> service.deletePreparedSegment(segmentId));

        assertNotNull(thrown.getSuppressed());
        assertTrue(thrown.getSuppressed().length > 0);
    }

    private static SegmentId openSourceSegment(
            final SegmentRegistry<Integer, String> registry) {
        return openSourceSegment(registry, entries(6));
    }

    private static SegmentId openSourceSegment(
            final SegmentRegistry<Integer, String> registry,
            final List<Entry<Integer, String>> entries) {
        final SegmentId segmentId = registry.materialization().nextSegmentId();
        final var writerTx = registry.materialization().openWriterTx(segmentId);
        try (var writer = writerTx.open()) {
            entries.forEach(writer::write);
        }
        writerTx.commit();
        return segmentId;
    }

    private static List<Entry<Integer, String>> entries(final int count) {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"), Entry.of(5, "e"), Entry.of(6, "f"),
                Entry.of(7, "g")).subList(0, count);
    }

    private static void assertPreparedChildrenDeleted(final Directory directory,
            final SegmentId sourceSegmentId) {
        assertFalse(directory.isFileExists(
                SegmentId.of(sourceSegmentId.getId() + 1).getName()));
        assertFalse(directory.isFileExists(
                SegmentId.of(sourceSegmentId.getId() + 2).getName()));
    }

    @SuppressWarnings("unchecked")
    private static SegmentFullWriterTx<Integer, String> writerTx() {
        return mock(SegmentFullWriterTx.class);
    }

    @SuppressWarnings("unchecked")
    private static EntryWriter<Integer, String> writer() {
        return mock(EntryWriter.class);
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
                .withConfiguration(effective(conf))
                .withSegmentMaintenanceExecutor(stableSegmentMaintenancePool)
                .withRegistryMaintenanceExecutor(registryMaintenancePool)
                .build();
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))//
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))//
                .segment(segment -> segment.cacheKeyLimit(10))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(10))//
                .segment(segment -> segment.chunkKeyLimit(4))//
                .segment(segment -> segment.deltaCacheFileLimit(2))//
                .segment(segment -> segment.maxKeys(50))//
                .segment(segment -> segment.cachedSegmentLimit(5))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(128))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))//
                .maintenance(maintenance -> maintenance.indexThreads(1))//
                .maintenance(maintenance -> maintenance.busyBackoffMillis(1))//
                .maintenance(maintenance -> maintenance.busyTimeoutMillis(1000))//
                .logging(logging -> logging.contextEnabled(false))//
                .identity(identity -> identity.name("split-materialization-service-test"))//
                .build();
    }
}
