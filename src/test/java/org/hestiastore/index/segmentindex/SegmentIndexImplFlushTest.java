package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentIndexImplFlushTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Test
    void flushCacheIfNeeded_skipsFlushWhenAtOrBelowLimit() {
        final TestableIndex index = new TestableIndex(
                buildConfWithCacheLimit(2), new MemDirectory());

        index.put(1, "one");
        index.put(2, "two"); // cache size == limit
        index.flushCacheIfNeeded(); // explicit call to cover method directly

        assertEquals(0, index.flushCalls);
        index.close();
    }

    @Test
    void flushCacheIfNeeded_triggersFlushWhenPutExceedsLimit() {
        final TestableIndex index = new TestableIndex(
                buildConfWithCacheLimit(2), new MemDirectory());

        index.put(1, "one");
        index.put(2, "two");
        index.put(3, "three"); // exceeds limit

        assertEquals(1, index.flushCalls);
        index.close();
    }

    @Test
    void deleteUsesFlushCacheIfNeededWhenLimitExceeded() {
        final TestableIndex index = new TestableIndex(
                buildConfWithCacheLimit(1), new MemDirectory());

        index.delete(1); // cache size == limit
        index.delete(2); // exceeds limit

        assertEquals(1, index.flushCalls);
        index.close();
    }

    @Test
    void flushKeepsEntriesAddedAfterSnapshot() throws Exception {
        final IndexConfiguration<Integer, String> conf = buildConfWithCacheLimit(
                4);
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                new MemDirectory(), tdi, tds, conf);
        final Entry<Integer, String> lateEntry = Entry.of(99, "late");
        final UniqueCache<Integer, String> cache = new LateEntryCache(
                tdi.getComparator(), conf.getMaxNumberOfKeysInCache(),
                lateEntry);
        replaceCache(index, cache);

        index.put(1, "one");
        index.put(2, "two");

        index.flush();

        assertEquals("late", index.get(99));
        index.close();
    }

    private IndexConfiguration<Integer, String> buildConfWithCacheLimit(
            final int maxKeysInCache) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("test-index")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(maxKeysInCache)//
                .withMaxNumberOfKeysInSegment(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private final class TestableIndex
            extends IndexInternalDefault<Integer, String> {

        private int flushCalls = 0;

        TestableIndex(final IndexConfiguration<Integer, String> conf,
                final Directory directory) {
            super(directory, tdi, tds, conf);
        }

        @Override
        protected void flushCache() {
            flushCalls++;
        }
    }

    private static void replaceCache(final SegmentIndexImpl<?, ?> index,
            final UniqueCache<?, ?> cache) throws Exception {
        final Field cacheField = SegmentIndexImpl.class
                .getDeclaredField("cache");
        cacheField.setAccessible(true);
        cacheField.set(index, cache);
    }

    private static final class LateEntryCache
            extends UniqueCache<Integer, String> {

        private final Entry<Integer, String> lateEntry;
        private boolean inserted = false;

        LateEntryCache(final java.util.Comparator<Integer> comparator,
                final int initialCapacity,
                final Entry<Integer, String> lateEntry) {
            super(comparator, initialCapacity);
            this.lateEntry = lateEntry;
        }

        @Override
        public List<Entry<Integer, String>> getAsSortedList() {
            final List<Entry<Integer, String>> snapshot = super
                    .getAsSortedList();
            insertLateEntry();
            return snapshot;
        }

        @Override
        public List<Entry<Integer, String>> snapshotAndClear() {
            final List<Entry<Integer, String>> snapshot = super
                    .snapshotAndClear();
            insertLateEntry();
            return snapshot;
        }

        private void insertLateEntry() {
            if (inserted) {
                return;
            }
            inserted = true;
            put(lateEntry);
        }
    }
}
