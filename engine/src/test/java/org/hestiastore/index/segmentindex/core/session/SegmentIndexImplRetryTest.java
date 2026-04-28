package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.OperationResult;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
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
                return OperationResult.busy();
            }
            return OperationResult.ok("value");
        });

        replaceSegment(registry, segmentId, segment);

        try {
            assertEquals("value", index.get(1));
            verify(segment, times(2)).get(1);
        } finally {
            replaceSegment(registry, segmentId, original);
        }
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
                .identity(identity -> identity.keyClass(Integer.class)).identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi)).identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-retry-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(1))
                .writePath(writePath -> writePath.legacyImmutableRunLimit(1))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(2))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(2))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
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
            stateField.set(entry, enumConstant(stateClass, "READY"));
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

    private static Object enumConstant(final Class<?> enumClass,
            final String name) {
        for (final Object constant : enumClass.getEnumConstants()) {
            if (((Enum<?>) constant).name().equals(name)) {
                return constant;
            }
        }
        throw new IllegalArgumentException("Unknown enum constant: " + name);
    }
}
