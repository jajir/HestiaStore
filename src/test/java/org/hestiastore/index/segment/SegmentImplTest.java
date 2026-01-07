package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.bloomfilter.BloomFilterWriterTx;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriterTx;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@org.mockito.junit.jupiter.MockitoSettings(strictness = org.mockito.quality.Strictness.LENIENT)
class SegmentImplTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private VersionController versionController;
    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;
    @Mock
    private SegmentResources<Integer, String> segmentDataProvider;
    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;
    @Mock
    private SegmentDeltaCacheWriter<Integer, String> deltaCacheWriter;
    @Mock
    private SegmentSearcher<Integer, String> segmentSearcher;
    @Mock
    private SegmentId segmentId;
    @Mock
    private SegmentStats stats;
    @Mock
    private SegmentDeltaCache<Integer, String> deltaCache;
    @Mock
    private BloomFilter<Integer> bloomFilter;
    @Mock
    private org.hestiastore.index.directory.Directory directory;
    @Mock
    private org.hestiastore.index.directory.FileReaderSeekable seekableReader;
    @Mock
    private BloomFilterWriter<Integer> bloomFilterWriter;
    @Mock
    private BloomFilterWriterTx<Integer> bloomFilterWriterTx;
    @Mock
    private ScarceSegmentIndex<Integer> scarceIndex;
    @Mock
    private ChunkEntryFile<Integer, String> chunkPairFile;
    @Mock
    private ChunkEntryFileWriterTx<Integer, String> chunkEntryWriterTx;
    @Mock
    private ScarceIndexWriterTx<Integer> scarceWriterTx;
    @Mock
    private EntryWriter<Integer, Integer> scarceEntryWriter;
    @Mock
    private ChunkEntryFileWriter<Integer, String> chunkEntryWriter;
    @Mock
    private EntryIteratorWithCurrent<Integer, String> indexIterator;

    private SegmentConf conf;
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private SegmentImpl<Integer, String> subject;

    @BeforeEach
    void setUpSubject() {
        conf = new SegmentConf(50, 100, 1000, 3, 0, 0, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        when(segmentFiles.getId()).thenReturn(segmentId);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(tds);
        when(segmentFiles.getIndexFile()).thenReturn(chunkPairFile);
        when(segmentFiles.getIndexFileName()).thenReturn("segment.index");
        when(segmentFiles.getAsyncDirectory()).thenReturn(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory));
        when(directory.isFileExists("segment.index")).thenReturn(true);
        when(directory.getFileReaderSeekable("segment.index"))
                .thenReturn(seekableReader);
        when(chunkPairFile.openWriterTx()).thenReturn(chunkEntryWriterTx);
        when(chunkEntryWriterTx.openWriter()).thenReturn(chunkEntryWriter);
        when(chunkPairFile.openIterator()).thenReturn(indexIterator);
        when(segmentFiles.getScarceIndex()).thenReturn(scarceIndex);
        when(scarceIndex.openWriterTx()).thenReturn(scarceWriterTx);
        when(scarceWriterTx.open()).thenReturn(scarceEntryWriter);
        doNothing().when(scarceIndex).loadCache();
        when(deltaCacheController.getDeltaCache()).thenReturn(deltaCache);
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(deltaCache.getAsSortedList()).thenReturn(List.of());
        when(indexIterator.hasNext()).thenReturn(false);
        when(segmentDataProvider.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomFilterWriterTx);
        when(bloomFilterWriterTx.open()).thenReturn(bloomFilterWriter);
        doNothing().when(segmentDataProvider).invalidate();
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                versionController);
        subject = new SegmentImpl<>(segmentFiles, conf, versionController,
                segmentPropertiesManager, segmentDataProvider,
                deltaCacheController, segmentSearcher, compacter);
    }

    @Test
    void getStats_and_getNumberOfKeys_delegate() {
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(stats);
        when(stats.getNumberOfKeys()).thenReturn(7L);
        assertSame(stats, subject.getStats());
        assertEquals(7L, subject.getNumberOfKeys());
    }

    @Test
    void put_writes_to_segment_cache_and_bumps_version() {
        assertEquals(SegmentResultStatus.OK,
                subject.put(1, "A").getStatus());
        assertEquals(SegmentResultStatus.OK,
                subject.put(2, "B").getStatus());

        verify(versionController, org.mockito.Mockito.times(2)).changeVersion();
        final SegmentResult<String> first = subject.get(1);
        final SegmentResult<String> second = subject.get(2);
        assertEquals(SegmentResultStatus.OK, first.getStatus());
        assertEquals(SegmentResultStatus.OK, second.getStatus());
        assertEquals("A", first.getValue());
        assertEquals("B", second.getValue());
        assertEquals(2, subject.getNumberOfKeysInWriteCache());
    }

    @Test
    void put_rejects_nulls() {
        assertThrows(IllegalArgumentException.class,
                () -> subject.put(null, "A"));
        assertThrows(IllegalArgumentException.class,
                () -> subject.put(1, null));
    }

    @Test
    void flush_noop_when_write_cache_empty() {
        assertEquals(SegmentResultStatus.OK, subject.flush().getStatus());

        verify(versionController, never()).changeVersion();
    }

    @Test
    void get_uses_segment_cache_when_present() {
        assertEquals(SegmentResultStatus.OK,
                subject.put(123, "val").getStatus());
        final SegmentResult<String> result = subject.get(123);
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertEquals("val", result.getValue());
        verify(segmentSearcher, never()).get(any(), any(), any());
    }

    @Test
    void get_returns_null_for_tombstone_without_search() {
        assertEquals(SegmentResultStatus.OK,
                subject.put(123, tds.getTombstone()).getStatus());
        final SegmentResult<String> result = subject.get(123);
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertNull(result.getValue());
        verify(segmentSearcher, never()).get(any(), any(), any());
    }

    @Test
    void get_falls_back_to_searcher_on_cache_miss() {
        when(segmentSearcher.get(eq(123), eq(segmentDataProvider), any()))
                .thenReturn("val");

        final SegmentResult<String> result = subject.get(123);
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertEquals("val", result.getValue());
        verify(segmentSearcher).get(eq(123), eq(segmentDataProvider), any());
    }

    @Test
    void segmentIndexSearcher_is_lazy_and_cached() {
        final SegmentIndexSearcher<Integer, String> first = subject
                .getSegmentIndexSearcher();
        final SegmentIndexSearcher<Integer, String> second = subject
                .getSegmentIndexSearcher();
        assertSame(first, second);
    }

    @Test
    void resetSegmentIndexSearcher_recreates_instance() {
        final SegmentIndexSearcher<Integer, String> first = subject
                .getSegmentIndexSearcher();
        subject.resetSegmentIndexSearcher();
        final SegmentIndexSearcher<Integer, String> second = subject
                .getSegmentIndexSearcher();
        assertNotSame(first, second);
    }

    @Test
    void flush_changes_version_when_entries_present() {
        assertEquals(SegmentResultStatus.OK,
                subject.put(1, "A").getStatus());
        org.mockito.Mockito.reset(versionController);
        assertEquals(SegmentResultStatus.OK, subject.flush().getStatus());

        verify(versionController).changeVersion();
    }

    @Test
    void openIterator_returns_non_null_and_closes() {
        when(indexIterator.hasNext()).thenReturn(false);
        final SegmentResult<EntryIterator<Integer, String>> result = subject
                .openIterator();
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertNotNull(result.getValue());
        try (EntryIterator<Integer, String> it = result.getValue()) {
            assertNotNull(it);
            assertFalse(it.hasNext());
        }
    }

    @Test
    void identity() {
        when(versionController.getVersion()).thenReturn(42);
        assertSame(segmentId, subject.getId());
    }

    @Test
    void compact_invokes_compacter_and_commits() {
        when(segmentPropertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of());
        assertEquals(SegmentResultStatus.OK, subject.compact().getStatus());
        verify(chunkEntryWriterTx).commit();
        verify(scarceWriterTx).commit();
    }

    @Test
    void checkAndRepairConsistency_empty_returns_null() {
        when(indexIterator.hasNext()).thenReturn(false);
        assertNull(subject.checkAndRepairConsistency());
    }

    @Test
    void checkAndRepairConsistency_invalid_order_throws() {
        when(indexIterator.hasNext()).thenReturn(true, true, true, false);
        when(indexIterator.next()).thenReturn(Entry.of(1, "a"))
                .thenReturn(Entry.of(3, "b")).thenReturn(Entry.of(2, "c"));
        assertThrows(org.hestiastore.index.IndexException.class,
                () -> subject.checkAndRepairConsistency());
    }
}
