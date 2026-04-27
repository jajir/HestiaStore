package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryBuilderTest {

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Test
    void builderRejectsNullDirectory() {
        final SegmentRegistryBuilder<Integer, String> builder = SegmentRegistry
                .<Integer, String>builder();
        assertThrows(IllegalArgumentException.class,
                () -> builder.withDirectoryFacade((Directory) null));
    }

    @Test
    void builderRejectsNullKeyDescriptor() {
        final SegmentRegistryBuilder<Integer, String> builder = SegmentRegistry
                .<Integer, String>builder();
        assertThrows(IllegalArgumentException.class,
                () -> builder.withKeyTypeDescriptor(null));
    }

    @Test
    void builderRejectsNullValueDescriptor() {
        final SegmentRegistryBuilder<Integer, String> builder = SegmentRegistry
                .<Integer, String>builder();
        assertThrows(IllegalArgumentException.class,
                () -> builder.withValueTypeDescriptor(null));
    }

    @Test
    void builderRejectsNullConfiguration() {
        final SegmentRegistryBuilder<Integer, String> builder = SegmentRegistry
                .<Integer, String>builder();
        assertThrows(IllegalArgumentException.class,
                () -> builder.withConfiguration(null));
    }

    @Test
    void builderRejectsNullSegmentMaintenanceExecutor() {
        final SegmentRegistryBuilder<Integer, String> builder = SegmentRegistry
                .<Integer, String>builder();
        assertThrows(IllegalArgumentException.class,
                () -> builder.withSegmentMaintenanceExecutor(null));
    }

    @Test
    void builderRejectsNullRegistryMaintenanceExecutor() {
        final SegmentRegistryBuilder<Integer, String> builder = SegmentRegistry
                .<Integer, String>builder();
        assertThrows(IllegalArgumentException.class,
                () -> builder.withRegistryMaintenanceExecutor(null));
    }

    @Test
    void builderUsesDefaultWiring() {
        final MemDirectory directory = new MemDirectory();
        directory.mkdir("segment-00005");
        final Directory asyncDirectory = directory;
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final ExecutorService stableSegmentMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final SegmentRegistry<Integer, String> registry = SegmentRegistry
                    .<Integer, String>builder()
                    .withDirectoryFacade(asyncDirectory)
                    .withKeyTypeDescriptor(new TypeDescriptorInteger())
                    .withValueTypeDescriptor(new TypeDescriptorShortString())
                    .withConfiguration(conf)
                    .withSegmentMaintenanceExecutor(
                            stableSegmentMaintenanceExecutor)
                    .withRegistryMaintenanceExecutor(
                            registryMaintenanceExecutor)
                    .build();
            try {
                final SegmentRegistryImpl<Integer, String> impl = (SegmentRegistryImpl<Integer, String>) registry;
                assertEquals(SegmentId.of(6),
                        impl.allocateSegmentId().getValue());
                assertNotNull(readField(impl, "cache"),
                        "Expected prebuilt cache wiring");
                assertNotNull(readField(impl, "segmentIdAllocator"),
                        "Expected default segment id allocator");
            } finally {
                registry.close();
            }
        } finally {
            stableSegmentMaintenanceExecutor.shutdownNow();
            registryMaintenanceExecutor.shutdownNow();
        }
    }

    @Test
    void buildFailsWhenRegistryMaintenanceExecutorIsMissing() {
        final MemDirectory directory = new MemDirectory();
        final Directory asyncDirectory = directory;
        final ExecutorService stableSegmentMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> buildWithoutRegistryMaintenanceExecutor(asyncDirectory,
                            stableSegmentMaintenanceExecutor));
            assertTrue(ex.getMessage()
                    .contains(
                            "Property 'registryMaintenanceExecutor' must not be null."));
        } finally {
            stableSegmentMaintenanceExecutor.shutdownNow();
        }
    }

    @Test
    void closeFlushesDirtySegmentWhenRegistryFreezes() {
        final MemDirectory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final SegmentRegistry<Integer, String> registry = SegmentRegistry
                    .<Integer, String>builder()
                    .withDirectoryFacade(directory)
                    .withKeyTypeDescriptor(new TypeDescriptorInteger())
                    .withValueTypeDescriptor(new TypeDescriptorShortString())
                    .withConfiguration(newConfiguration())
                    .withSegmentMaintenanceExecutor(
                            stableSegmentMaintenanceExecutor)
                    .withRegistryMaintenanceExecutor(
                            registryMaintenanceExecutor)
                    .build();
            final BlockingSegment<Integer, String> created = registry
                    .createSegment();
            assertNotNull(created);
            assertSame(OperationStatus.OK,
                    created.getSegment().put(1, "value").getStatus());
            registry.close();
        } finally {
            stableSegmentMaintenanceExecutor.shutdownNow();
            registryMaintenanceExecutor.shutdownNow();
        }
    }

    @Test
    void createBlockingSegmentReturnsBlockingAccessToCreatedSegment() {
        final MemDirectory directory = new MemDirectory();
        final ExecutorService stableSegmentMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService registryMaintenanceExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final SegmentRegistry<Integer, String> registry = SegmentRegistry
                    .<Integer, String>builder()
                    .withDirectoryFacade(directory)
                    .withKeyTypeDescriptor(new TypeDescriptorInteger())
                    .withValueTypeDescriptor(new TypeDescriptorShortString())
                    .withConfiguration(newConfiguration())
                    .withSegmentMaintenanceExecutor(
                            stableSegmentMaintenanceExecutor)
                    .withRegistryMaintenanceExecutor(
                            registryMaintenanceExecutor)
                    .build();
            try {
                final BlockingSegment<Integer, String> handle = registry
                        .createSegment();

                handle.put(1, "value");

                assertEquals("value", handle.get(1));
            } finally {
                registry.close();
            }
        } finally {
            stableSegmentMaintenanceExecutor.shutdownNow();
            registryMaintenanceExecutor.shutdownNow();
        }
    }

    private static Object readField(final Object target, final String name) {
        try {
            final Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(target);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Unable to read field " + name, e);
        }
    }

    private SegmentRegistry<Integer, String> buildWithoutRegistryMaintenanceExecutor(
            final Directory asyncDirectory,
            final ExecutorService stableSegmentMaintenanceExecutor) {
        return SegmentRegistry.<Integer, String>builder()
                .withDirectoryFacade(asyncDirectory)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withConfiguration(conf)
                .withSegmentMaintenanceExecutor(
                        stableSegmentMaintenanceExecutor)
                .build();
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-registry-builder-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withIndexBusyBackoffMillis(1)
                .withIndexBusyTimeoutMillis(10)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
