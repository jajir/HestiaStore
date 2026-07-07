package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.hestiastore.index.segmentregistry.SegmentRegistryCacheTestSupport.putReadyEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexSessionRetryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private SegmentIndex<Integer, String> index;

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
        index.maintenance().flushAndWait();

        final SegmentRouteMap<Integer> cache = readKeyToSegmentMap(
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
            index.maintenance().flushAndWait();
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

    @Test
    void putPastWriteCacheLimitSucceedsWhenAutomaticMaintenanceEnabled() {
        closeCurrentIndex();
        index = newIndex("write-cache-full-auto-enabled", true);

        for (int key = 0; key < 8; key++) {
            index.put(key, "value-" + key);
        }
        index.maintenance().flushAndWait();

        for (int key = 0; key < 8; key++) {
            assertEquals("value-" + key, index.get(key));
        }
    }

    @Test
    void putPastWriteCacheLimitFailsWhenAutomaticMaintenanceDisabled() {
        closeCurrentIndex();
        index = newIndex("write-cache-full-auto-disabled", false);

        index.put(1, "one");

        final IndexException exception = assertThrows(IndexException.class,
                () -> index.put(2, "two"));

        assertTrue(exception.getMessage().contains(
                "Write cache is full for segment"));
        assertTrue(exception.getMessage().contains(
                "automatic maintenance is disabled"));
    }

    private SegmentIndex<Integer, String> newIndex() {
        return newIndex("segment-index-retry-test", true);
    }

    private SegmentIndex<Integer, String> newIndex(final String indexName,
            final boolean automaticMaintenanceEnabled) {
        return newIndex(indexName, automaticMaintenanceEnabled, 5_000);
    }

    private SegmentIndex<Integer, String> newIndex(final String indexName,
            final boolean automaticMaintenanceEnabled,
            final int busyTimeoutMillis) {
        final IndexConfiguration<Integer, String> conf = buildConf(indexName,
                automaticMaintenanceEnabled, busyTimeoutMillis);
        return SegmentIndexSessionTestSupport.createStarted(
                new MemDirectory(), tdi, tds, effective(conf));
    }

    private IndexConfiguration<Integer, String> buildConf(final String indexName,
            final boolean automaticMaintenanceEnabled,
            final int busyTimeoutMillis) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)).identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi)).identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segmentConfig -> segmentConfig.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(1))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(2))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(2))
                .maintenance(maintenance -> maintenance.busyBackoffMillis(1))
                .maintenance(maintenance -> maintenance.busyTimeoutMillis(
                        busyTimeoutMillis))
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(
                        automaticMaintenanceEnabled))
                .segment(segmentConfig -> segmentConfig.chunkKeyLimit(2))
                .segment(segmentConfig -> segmentConfig.maxKeys(100))
                .segment(segmentConfig -> segmentConfig.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private void closeCurrentIndex() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    private static <K, V> void replaceSegment(
            final Object registry, final SegmentId segmentId,
            final Segment<K, V> segment) {
        final Object cache = readCache(registry);
        putReadyEntry(cache, segmentId, segment);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> readSegmentRegistry(
            final Object index) {
        return (SegmentRegistry<K, V>) SegmentIndexTestAccess
                .segmentRegistry(index);
    }

    private static <K, V> SegmentRouteMap<K> readKeyToSegmentMap(
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

}
