package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.SegmentId;
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
        assertThrows(IllegalArgumentException.class,
                () -> SegmentRegistry.<Integer, String>builder()
                        .withDirectoryFacade(null));
    }

    @Test
    void builderRejectsNullKeyDescriptor() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentRegistry.<Integer, String>builder()
                        .withKeyTypeDescriptor(null));
    }

    @Test
    void builderRejectsNullValueDescriptor() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentRegistry.<Integer, String>builder()
                        .withValueTypeDescriptor(null));
    }

    @Test
    void builderRejectsNullConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentRegistry.<Integer, String>builder()
                        .withConfiguration(null));
    }

    @Test
    void builderRejectsNullExecutor() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentRegistry.<Integer, String>builder()
                        .withMaintenanceExecutor(null));
    }

    @Test
    void builderUsesDefaultWiring() {
        final MemDirectory directory = new MemDirectory();
        directory.mkdir("segment-00005");
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(10);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final SegmentRegistry<Integer, String> registry = SegmentRegistry
                    .<Integer, String>builder()
                    .withDirectoryFacade(asyncDirectory)
                    .withKeyTypeDescriptor(new TypeDescriptorInteger())
                    .withValueTypeDescriptor(new TypeDescriptorShortString())
                    .withConfiguration(conf)
                    .withMaintenanceExecutor(executor)
                    .build();
            try {
                assertEquals(SegmentId.of(6),
                        registry.allocateSegmentId().getSegment().orElse(null));
                final SegmentRegistryImpl<Integer, String> impl = (SegmentRegistryImpl<Integer, String>) registry;
                assertNotNull(readField(impl, "segmentFactory"),
                        "Expected default segment factory");
                assertNotNull(readField(impl, "segmentIdAllocator"),
                        "Expected default segment id allocator");
            } finally {
                registry.close();
            }
        } finally {
            executor.shutdownNow();
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
}
