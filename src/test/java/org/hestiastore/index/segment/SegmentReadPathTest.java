package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SegmentReadPathTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private SegmentResources<Integer, String> segmentResources;
    @Mock
    private SegmentSearcher<Integer, String> segmentSearcher;
    @Mock
    private SegmentCache<Integer, String> segmentCache;
    @Mock
    private VersionController versionController;
    @Mock
    private ChunkEntryFile<Integer, String> chunkEntryFile;
    @Mock
    private AsyncDirectory asyncDirectory;
    @Mock
    private EntryIteratorWithCurrent<Integer, String> baseIterator;

    private SegmentReadPath<Integer, String> subject;
    private final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();

    @BeforeEach
    void setUp() {
        final SegmentConf conf = new SegmentConf(1, 1, 1, 1, 0, 0, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        when(segmentFiles.getIndexFile()).thenReturn(chunkEntryFile);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(keyDescriptor);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(valueDescriptor);
        when(segmentFiles.getId()).thenReturn(SegmentId.of(1));
        when(segmentFiles.getIndexFileName()).thenReturn("segment.index");
        when(segmentFiles.getAsyncDirectory()).thenReturn(asyncDirectory);
        when(asyncDirectory.isFileExistsAsync("segment.index")).thenReturn(
                CompletableFuture.completedFuture(false));
        when(segmentCache.getAsSortedList()).thenReturn(List.of());
        when(chunkEntryFile.openIterator()).thenReturn(baseIterator);
        when(baseIterator.hasNext()).thenReturn(false);
        subject = new SegmentReadPath<>(segmentFiles, conf, segmentResources,
                segmentSearcher, segmentCache, versionController);
    }

    @Test
    void openIterator_failFast_wraps_with_lock() {
        try (EntryIterator<Integer, String> iterator = subject
                .openIterator(SegmentIteratorIsolation.FAIL_FAST)) {
            assertTrue(iterator instanceof EntryIteratorWithLock);
        }
    }

    @Test
    void openIterator_fullIsolation_returns_merged_iterator() {
        try (EntryIterator<Integer, String> iterator = subject
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertFalse(iterator instanceof EntryIteratorWithLock);
        }
    }

    @Test
    void get_returns_cached_value_without_search() {
        when(segmentCache.get(1)).thenReturn("value");

        assertEquals("value", subject.get(1));

        verify(segmentSearcher, never()).get(any(), any(), any());
    }

    @Test
    void get_returns_null_for_cached_tombstone() {
        when(segmentCache.get(1)).thenReturn(valueDescriptor.getTombstone());

        assertNull(subject.get(1));

        verify(segmentSearcher, never()).get(any(), any(), any());
    }

    @Test
    void get_delegates_to_searcher_on_cache_miss() {
        when(segmentCache.get(1)).thenReturn(null);
        when(segmentSearcher.get(eq(1), eq(segmentResources), any()))
                .thenReturn("value");

        assertEquals("value", subject.get(1));

        verify(segmentSearcher).get(eq(1), eq(segmentResources), any());
    }

    @Test
    void getSegmentIndexSearcher_is_cached_and_resettable() {
        final SegmentIndexSearcher<Integer, String> first = subject
                .getSegmentIndexSearcher();
        final SegmentIndexSearcher<Integer, String> second = subject
                .getSegmentIndexSearcher();

        assertNotNull(first);
        assertSame(first, second);

        subject.resetSegmentIndexSearcher();

        final SegmentIndexSearcher<Integer, String> third = subject
                .getSegmentIndexSearcher();
        assertNotNull(third);
        assertNotSame(first, third);
    }
}
