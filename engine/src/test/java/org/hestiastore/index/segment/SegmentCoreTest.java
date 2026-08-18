package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentCoreTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private VersionController versionController;
    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;
    @Mock
    private SegmentCache<Integer, String> segmentCache;
    @Mock
    private SegmentReadPath<Integer, String> readPath;
    @Mock
    private SegmentWritePath<Integer, String> writePath;
    @Mock
    private SegmentMaintenancePath<Integer, String> maintenancePath;
    @Mock
    private ChunkEntryFile<Integer, String> indexFile;
    @Mock
    private TypeDescriptor<String> conditionalValueDescriptor;

    private final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();

    private SegmentCore<Integer, String> core;
    private boolean coreClosed;

    @BeforeEach
    void setUp() {
        core = new SegmentCore<>(segmentFiles, versionController,
                segmentPropertiesManager, segmentCache, readPath, writePath,
                maintenancePath);
        coreClosed = false;
    }

    @AfterEach
    void tearDown() {
        if (core != null && !coreClosed) {
            core.close();
            coreClosed = true;
        }
    }

    @Test
    void invalidateIteratorsBumpsVersion() {
        core.invalidateIterators();
        verify(versionController).changeVersion();
    }

    @Test
    void close_evictsCacheBeforeClosingReadResources() {
        core.close();
        coreClosed = true;

        final InOrder closeOrder = inOrder(segmentCache, readPath);
        closeOrder.verify(segmentCache).evictAll();
        closeOrder.verify(readPath).close();
    }

    @Test
    void close_closesReadResourcesWhenCacheEvictionFails() {
        doThrow(new RuntimeException("eviction failed")).when(segmentCache)
                .evictAll();

        assertThrows(RuntimeException.class, core::close);
        coreClosed = true;

        verify(readPath).close();
    }

    @Test
    void tryPutWithoutWaitingUpdatesWriteCacheCount() {
        when(writePath.getNumberOfKeysInWriteCache()).thenReturn(0, 1);
        when(writePath.tryPutWithoutWaiting(1, "one")).thenReturn(true);

        assertEquals(0, core.getNumberOfKeysInWriteCache());
        assertTrue(core.tryPutWithoutWaiting(1, "one"));
        assertEquals(1, core.getNumberOfKeysInWriteCache());
    }

    @Test
    void replace_uses_atomic_active_write_cache_replacement() {
        when(segmentFiles.getValueTypeDescriptor())
                .thenReturn(conditionalValueDescriptor);
        when(conditionalValueDescriptor.getComparator())
                .thenReturn(String.CASE_INSENSITIVE_ORDER);
        when(segmentCache.getFromWriteCache(1)).thenReturn("ONE");
        when(segmentCache.replaceInWriteCache(1, "ONE", "new"))
                .thenReturn(true);

        final OperationResult<Boolean> result = core
                .tryReplaceWithoutWaiting(1, "one", "new");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertTrue(result.getValue());
        verify(readPath, never()).get(1);
    }

    @Test
    void replace_returns_false_for_mismatched_active_value() {
        when(segmentFiles.getValueTypeDescriptor())
                .thenReturn(valueDescriptor);
        when(segmentCache.getFromWriteCache(1)).thenReturn("other");

        final OperationResult<Boolean> result = core
                .tryReplaceWithoutWaiting(1, "one", "new");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertFalse(result.getValue());
        verify(segmentCache, never()).replaceInWriteCache(1, "other", "new");
    }

    @Test
    void putIfAbsent_inserts_after_complete_logical_read() {
        when(segmentFiles.getValueTypeDescriptor())
                .thenReturn(valueDescriptor);
        when(readPath.get(1)).thenReturn(null);
        when(segmentCache.tryPutIfAbsentToWriteCacheWithoutWaiting(
                Entry.of(1, "one"))).thenReturn(true);

        final OperationResult<Boolean> result = core
                .tryPutIfAbsentWithoutWaiting(1, "one");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertTrue(result.getValue());
        verify(readPath).get(1);
    }

    @Test
    void putIfAbsent_reports_full_cache_when_insert_cannot_reserve_capacity() {
        when(segmentFiles.getValueTypeDescriptor())
                .thenReturn(valueDescriptor);
        when(readPath.get(1)).thenReturn(null);

        final OperationResult<Boolean> result = core
                .tryPutIfAbsentWithoutWaiting(1, "one");

        assertEquals(OperationStatus.WRITE_CACHE_FULL, result.getStatus());
    }

    @Test
    void snapshotCacheEntries_returns_sorted_entries() {
        final List<Entry<Integer, String>> snapshot = List.of(Entry.of(1, "a"),
                Entry.of(2, "b"));
        when(segmentCache.getAsSortedList()).thenReturn(snapshot);

        final List<Entry<Integer, String>> result = core.snapshotCacheEntries();

        assertEquals(List.of(Entry.of(1, "a"), Entry.of(2, "b")), result);
    }

    @Test
    void openIteratorFromCompactionSnapshot_reads_snapshot_entries()
    {
        final List<Entry<Integer, String>> snapshot = List.of(Entry.of(2, "b"));
        when(segmentFiles.getIndexFile()).thenReturn(indexFile);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(keyDescriptor);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(valueDescriptor);
        when(segmentCache.compactionSnapshotIterator())
                .thenReturn(snapshot.iterator());
        when(indexFile.openIterator())
                .thenReturn(new SimpleEntryIteratorWithCurrent<>(List
                        .of(Entry.of(1, "a"), Entry.of(3, "c")).iterator()));

        try (EntryIterator<Integer, String> iterator = core
                .openIteratorFromCompactionSnapshot()) {
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "a"), iterator.next());
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(2, "b"), iterator.next());
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(3, "c"), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    private static final class SimpleEntryIteratorWithCurrent<K, V>
            extends AbstractCloseableResource
            implements EntryIteratorWithCurrent<K, V> {

        private final Iterator<Entry<K, V>> iterator;
        private Entry<K, V> current;

        private SimpleEntryIteratorWithCurrent(
                final Iterator<Entry<K, V>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            current = iterator.next();
            return current;
        }

        @Override
        public Optional<Entry<K, V>> getCurrent() {
            return Optional.ofNullable(current);
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}
