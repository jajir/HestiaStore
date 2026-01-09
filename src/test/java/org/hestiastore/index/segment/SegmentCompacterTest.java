package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.junit.jupiter.api.BeforeEach;
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

    private SegmentCompacter<Integer, String> sc;

    @BeforeEach
    void setUp() {
        sc = new SegmentCompacter<>(versionController);
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
        when(segment.openIteratorFromSnapshot(snapshot)).thenReturn(iterator);

        final List<Entry<Integer, String>> written = new ArrayList<>();
        doAnswer(invocation -> {
            final WriterFunction<Integer, String> writerFunction = invocation
                    .getArgument(0);
            writerFunction.apply(new EntryWriter<>() {
                private boolean closed;

                @Override
                public void write(final Entry<Integer, String> entry) {
                    written.add(entry);
                }

                @Override
                public boolean wasClosed() {
                    return closed;
                }

                @Override
                public void close() {
                    if (closed) {
                        throw new IllegalStateException(
                                "EntryWriter already closed");
                    }
                    closed = true;
                }
            });
            return null;
        }).when(segment).executeFullWriteTx(any());

        sc.compact(segment, snapshot);

        assertEquals(snapshot, written);
        verify(segment).openIteratorFromSnapshot(snapshot);
        verify(versionController).changeVersion();
    }

}
