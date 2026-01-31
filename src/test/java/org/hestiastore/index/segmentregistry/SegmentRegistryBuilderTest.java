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
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> SegmentRegistry.builder(null,
                            new TypeDescriptorInteger(),
                            new TypeDescriptorShortString(), conf, executor));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void builderRejectsNullKeyDescriptor() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final AsyncDirectory directory = AsyncDirectoryAdapter
                    .wrap(new MemDirectory());
            assertThrows(IllegalArgumentException.class,
                    () -> SegmentRegistry.builder(directory, null,
                            new TypeDescriptorShortString(), conf, executor));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void builderRejectsNullValueDescriptor() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final AsyncDirectory directory = AsyncDirectoryAdapter
                    .wrap(new MemDirectory());
            assertThrows(IllegalArgumentException.class,
                    () -> SegmentRegistry.builder(directory,
                            new TypeDescriptorInteger(), null, conf, executor));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void builderRejectsNullConfiguration() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final AsyncDirectory directory = AsyncDirectoryAdapter
                    .wrap(new MemDirectory());
            assertThrows(IllegalArgumentException.class,
                    () -> SegmentRegistry.builder(directory,
                            new TypeDescriptorInteger(),
                            new TypeDescriptorShortString(), null, executor));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void builderRejectsNullExecutor() {
        final AsyncDirectory directory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        assertThrows(IllegalArgumentException.class,
                () -> SegmentRegistry.builder(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), conf, null));
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
            final SegmentRegistryImpl<Integer, String> registry = SegmentRegistry
                    .builder(asyncDirectory, new TypeDescriptorInteger(),
                            new TypeDescriptorShortString(), conf, executor)
                    .build();
            try {
                assertEquals(SegmentId.of(6),
                        registry.allocateSegmentId().getValue());
                assertNotNull(readField(registry, "segmentFactory"),
                        "Expected default segment factory");
                assertNotNull(readField(registry, "segmentIdAllocator"),
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
