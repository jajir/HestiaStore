package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentCompacterTest {

    @Mock
    private SegmentCore<Integer, String> segment;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentFullWriterTx<Integer, String> writerTx;

    private SegmentCompacter<Integer, String> sc;

    @BeforeEach
    void setUp() {
        sc = new SegmentCompacter<>(versionController);
    }

    @AfterEach
    void tearDown() {
        sc = null;
    }

    @Test
    void test_basic_operations() {
        assertNotNull(sc);
    }

    @Test
    void prepareCompaction_snapshots_cache() {
        final List<Entry<Integer, String>> snapshot = List
                .of(Entry.of(1, "a"));
        when(segment.snapshotCacheEntries()).thenReturn(snapshot);
        when(segment.getId()).thenReturn(SegmentId.of(1));

        final List<Entry<Integer, String>> result = sc
                .prepareCompaction(segment);

        assertEquals(snapshot, result);
        verify(segment).resetSegmentIndexSearcher();
        verify(segment).freezeWriteCacheForFlush();
        verify(segment).snapshotCacheEntries();
    }

    @Test
    void compact_writes_snapshot_entries_and_bumps_version() {
        when(segment.getId()).thenReturn(SegmentId.of(1));
        final List<Entry<Integer, String>> snapshot = List
                .of(Entry.of(1, "a"), Entry.of(2, "b"));
        final EntryIterator<Integer, String> iterator = EntryIterator
                .make(snapshot.iterator());
        final RecordingWriter<Integer, String> writer = new RecordingWriter<>();
        when(segment.openIteratorFromSnapshot(snapshot)).thenReturn(iterator);
        when(segment.openFullWriteTx()).thenReturn(writerTx);
        when(writerTx.open()).thenReturn(writer);

        sc.compact(segment, snapshot);

        assertEquals(snapshot, writer.getWritten());
        assertTrue(writer.wasClosed());
        verify(segment).openIteratorFromSnapshot(snapshot);
        verify(writerTx).commit();
        verify(versionController).changeVersion();
    }

    private static final class RecordingWriter<K, V> implements EntryWriter<K, V> {

        private final List<Entry<K, V>> written = new ArrayList<>();
        private boolean closed;

        @Override
        public void write(final Entry<K, V> entry) {
            written.add(entry);
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }

        @Override
        public void close() {
            if (closed) {
                throw new IllegalStateException("EntryWriter already closed");
            }
            closed = true;
        }

        private List<Entry<K, V>> getWritten() {
            return written;
        }
    }

}
