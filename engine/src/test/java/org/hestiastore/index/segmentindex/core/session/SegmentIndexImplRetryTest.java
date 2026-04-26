package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexImplRetryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexInternalConcurrent<Integer, String> index;

    @Mock
    private Segment<Integer, String> segment;

    @BeforeEach
    void setUp() {
        index = newIndex();
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void getRetriesOnBusyThenOk() {
        index.put(1, "one");
        index.flushAndWait();

        final KeyToSegmentMap<Integer> cache = readKeyToSegmentMap(
                index);
        final SegmentId segmentId = cache.findSegmentIdForKey(1);
        final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                index);
        final Segment<Integer, String> original = registry.loadSegment(segmentId)
                .getSegment();

        final AtomicInteger attempts = new AtomicInteger();
        when(segment.get(1)).thenAnswer(invocation -> {
            if (attempts.getAndIncrement() == 0) {
                return SegmentResult.busy();
            }
            return SegmentResult.ok("value");
        });

        replaceSegment(registry, segmentId, segment);

        assertEquals("value", index.get(1));
        verify(segment, times(2)).get(1);

        replaceSegment(registry, segmentId, original);
    }

    @Test
    void putRetriesOnBusyPartitionUntilDrainCompletes() {
        index.put(1, "one");
        index.put(2, "two");
        final Thread drainThread = new Thread(() -> {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50L));
            index.flushAndWait();
        });
        drainThread.start();
        try {
            index.put(3, "three");
            assertEquals("three", index.get(3));
        } finally {
            try {
                drainThread.join(TimeUnit.SECONDS.toMillis(5L));
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting for drain thread", ex);
            }
        }
    }

    private IndexInternalConcurrent<Integer, String> newIndex() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        return new IndexInternalConcurrent<>(
                new MemDirectory(), tdi, tds,
                conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class).withValueClass(String.class)
                .withKeyTypeDescriptor(tdi).withValueTypeDescriptor(tds)
                .withName("segment-index-retry-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(1)
                .withMaxNumberOfImmutableRunsPerPartition(1)
                .withMaxNumberOfKeysInPartitionBuffer(2)
                .withMaxNumberOfKeysInIndexBuffer(2)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static <K, V> void replaceSegment(
            final Object registry, final SegmentId segmentId,
            final Segment<K, V> segment) {
        final Object cache = readCache(registry);
        putCacheEntry(cache, segmentId, segment);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> readSegmentRegistry(
            final Object index) {
        return (SegmentRegistry<K, V>) SegmentIndexTestAccess
                .segmentRegistry(index);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeyToSegmentMap<K> readKeyToSegmentMap(
            final Object index) {
        return SegmentIndexTestAccess.keyToSegmentMap(index);
    }

    private static Object readCache(final Object registry) {
        try {
            final Field cacheField = registry.getClass().getDeclaredField(
                    "cache");
            cacheField.setAccessible(true);
            return cacheField.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read registry cache for test", ex);
        }
    }

    private static void putCacheEntry(final Object cache,
            final SegmentId segmentId, final Segment<?, ?> segment) {
        try {
            final Field mapField = cache.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final java.util.concurrent.ConcurrentHashMap<SegmentId, Object> map = (java.util.concurrent.ConcurrentHashMap<SegmentId, Object>) mapField
                    .get(cache);
            final Class<?> entryClass = Class.forName(
                    "org.hestiastore.index.segmentregistry.SegmentRegistryCache$Entry");
            final var ctor = entryClass.getDeclaredConstructor(long.class);
            ctor.setAccessible(true);
            final Object entry = ctor.newInstance(0L);
            final Field stateField = entryClass.getDeclaredField("state");
            stateField.setAccessible(true);
            final Class<?> stateClass = Class.forName(
                    "org.hestiastore.index.segmentregistry.SegmentRegistryCache$EntryState");
            stateField.set(entry, Enum.valueOf(
                    stateClass.asSubclass(Enum.class), "READY"));
            final Field valueField = entryClass.getDeclaredField("value");
            valueField.setAccessible(true);
            valueField.set(entry, segment);
            map.put(segmentId, entry);
            final Field sizeField = cache.getClass().getDeclaredField("size");
            sizeField.setAccessible(true);
            final java.util.concurrent.atomic.AtomicInteger size = (java.util.concurrent.atomic.AtomicInteger) sizeField
                    .get(cache);
            size.incrementAndGet();
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to update registry cache for test", ex);
        }
    }
}
