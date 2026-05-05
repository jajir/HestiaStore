package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexReadFacade;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexInternalConcurrentTest {

    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = new IndexInternalConcurrent<>(
                new MemDirectory(),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void putGetAndStreamRoundTrip() {
        index.put(1, "one");
        index.put(2, "two");

        assertEquals("one", index.get(1));

        try (Stream<Entry<Integer, String>> stream = index
                .getStream(SegmentWindow.unbounded())) {
            final List<Entry<Integer, String>> entries = stream.toList();
            assertEquals(2, entries.size());
            assertTrue(entries.contains(Entry.of(1, "one")));
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))//
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity.name("index-internal-concurrent-test"))//
                .logging(logging -> logging.contextEnabled(false))//
                .segment(segment -> segment.cacheKeyLimit(10))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(100))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .build();
    }

    @Test
    void getStreamFullIsolationIsLazyAndClosesIterator() {
        final AtomicInteger hasNextCalls = new AtomicInteger();
        final AtomicInteger nextCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        final EntryIterator<Integer, String> iterator = new CountingIterator<>(
                List.of(Entry.of(1, "one")).iterator(), hasNextCalls, nextCalls,
                closeCalls);

        final RecordingIndex streamingIndex = new RecordingIndex(iterator,
                buildConf());
        try (Stream<Entry<Integer, String>> stream = streamingIndex.getStream(
                SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals(SegmentIteratorIsolation.FULL_ISOLATION,
                    streamingIndex.getLastIsolation());
            assertEquals(0, hasNextCalls.get());
            assertEquals(0, nextCalls.get());
            assertEquals(1, stream.count());
        } finally {
            streamingIndex.close();
        }

        assertEquals(1, closeCalls.get());
        assertTrue(hasNextCalls.get() > 0);
        assertTrue(nextCalls.get() > 0);
    }

    @Test
    void getStreamFailFastIsLazyAndClosesIterator() {
        final AtomicInteger hasNextCalls = new AtomicInteger();
        final AtomicInteger nextCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        final EntryIterator<Integer, String> iterator = new CountingIterator<>(
                List.of(Entry.of(1, "one"), Entry.of(2, "two")).iterator(),
                hasNextCalls, nextCalls, closeCalls);

        final RecordingIndex streamingIndex = new RecordingIndex(iterator,
                buildConf());
        try (Stream<Entry<Integer, String>> stream = streamingIndex.getStream(
                SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FAIL_FAST)) {
            assertEquals(SegmentIteratorIsolation.FAIL_FAST,
                    streamingIndex.getLastIsolation());
            assertEquals(0, hasNextCalls.get());
            assertEquals(0, nextCalls.get());
            assertEquals(Entry.of(1, "one"), stream.findFirst().orElseThrow());
        } finally {
            streamingIndex.close();
        }

        assertEquals(1, closeCalls.get());
        assertTrue(hasNextCalls.get() > 0);
        assertEquals(1, nextCalls.get());
    }

    @Test
    void failFastIteratorKeepsOrderedPrefixAcrossFlushAndWaitBoundary() {
        index.put(1, "one");
        index.put(2, "two");
        index.put(3, "three");
        index.maintenance().flushAndWait();

        final List<Entry<Integer, String>> expected = List.of(
                Entry.of(1, "one"), Entry.of(2, "two"),
                Entry.of(3, "three"));
        try (EntryIterator<Integer, String> iterator = index.openSegmentIterator(
                SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FAIL_FAST)) {
            final var consumed = new java.util.ArrayList<Entry<Integer, String>>();
            assertTrue(iterator.hasNext());
            consumed.add(iterator.next());
            index.maintenance().flushAndWait();
            consumed.addAll(consumeAll(iterator));

            assertTrue(consumed.size() > 0);
            assertEquals(expected.subList(0, consumed.size()), consumed);
        }
    }

    @Test
    void failFastIteratorKeepsOrderedPrefixAcrossCompactAndWaitBoundary() {
        index.put(1, "one");
        index.put(2, "two");
        index.put(3, "three");
        index.maintenance().flushAndWait();

        final List<Entry<Integer, String>> expected = List.of(
                Entry.of(1, "one"), Entry.of(2, "two"),
                Entry.of(3, "three"));
        try (EntryIterator<Integer, String> iterator = index.openSegmentIterator(
                SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FAIL_FAST)) {
            final var consumed = new java.util.ArrayList<Entry<Integer, String>>();
            assertTrue(iterator.hasNext());
            consumed.add(iterator.next());
            index.maintenance().compactAndWait();
            consumed.addAll(consumeAll(iterator));

            assertTrue(consumed.size() > 0);
            assertEquals(expected.subList(0, consumed.size()), consumed);
        }
    }

    private static <K, V> List<Entry<K, V>> consumeAll(
            final EntryIterator<K, V> iterator) {
        final var entries = new java.util.ArrayList<Entry<K, V>>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }

    private static final class RecordingIndex
            extends SegmentIndexImpl<Integer, String> {

        private final EntryIterator<Integer, String> iterator;
        private SegmentIteratorIsolation lastIsolation;

        private RecordingIndex(final EntryIterator<Integer, String> iterator,
                final IndexConfiguration<Integer, String> conf) {
            super(new TypeDescriptorInteger(), mockPointOperationFacade(),
                    mockReadFacade(), mock(MaintenanceService.class),
                    mockTrackedRunner(), mock(SegmentIndexMaintenance.class),
                    mockSessionOwner());
            this.iterator = iterator;
        }

        @Override
        public EntryIterator<Integer, String> openSegmentIterator(
                final SegmentWindow segmentWindow,
                final SegmentIteratorIsolation isolation) {
            lastIsolation = isolation;
            return iterator;
        }

        SegmentIteratorIsolation getLastIsolation() {
            return lastIsolation;
        }

    }

    private static final class CountingIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        private final Iterator<Entry<K, V>> iterator;
        private final AtomicInteger hasNextCalls;
        private final AtomicInteger nextCalls;
        private final AtomicInteger closeCalls;

        private CountingIterator(final Iterator<Entry<K, V>> iterator,
                final AtomicInteger hasNextCalls,
                final AtomicInteger nextCalls,
                final AtomicInteger closeCalls) {
            this.iterator = iterator;
            this.hasNextCalls = hasNextCalls;
            this.nextCalls = nextCalls;
            this.closeCalls = closeCalls;
        }

        @Override
        public boolean hasNext() {
            hasNextCalls.incrementAndGet();
            return iterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            nextCalls.incrementAndGet();
            return iterator.next();
        }

        @Override
        protected void doClose() {
            closeCalls.incrementAndGet();
        }
    }

    @SuppressWarnings("unchecked")
    private static SegmentIndexPointOperationFacade<Integer, String>
            mockPointOperationFacade() {
        return mock(SegmentIndexPointOperationFacade.class);
    }

    @SuppressWarnings("unchecked")
    private static SegmentIndexReadFacade<Integer, String> mockReadFacade() {
        return mock(SegmentIndexReadFacade.class);
    }

    @SuppressWarnings("unchecked")
    private static SegmentIndexTrackedOperationRunner<Integer, String>
            mockTrackedRunner() {
        return mock(SegmentIndexTrackedOperationRunner.class);
    }

    @SuppressWarnings("unchecked")
    private static SegmentIndexSessionOwner<Integer, String> mockSessionOwner() {
        return mock(SegmentIndexSessionOwner.class);
    }
}
