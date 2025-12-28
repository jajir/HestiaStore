package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
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
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;
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
    private SegmentSearcher<Integer, String> segmentSearcher;
    @Mock
    private SegmentCompactionPolicyWithManager compactionPolicy;
    @Mock
    private SegmentSplitterPolicy<Integer, String> splitterPolicy;
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
        conf = new SegmentConf(100, 200, 3, 0, 0, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        when(segmentFiles.getId()).thenReturn(segmentId);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(tds);
        when(segmentFiles.getIndexFile()).thenReturn(chunkPairFile);
        when(segmentFiles.getIndexFileName()).thenReturn("segment.index");
        when(segmentFiles.getDirectoryFacade())
                .thenReturn(DirectoryFacade.of(directory));
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
        when(deltaCache.getAsSortedList()).thenReturn(List.of());
        when(indexIterator.hasNext()).thenReturn(false);
        when(segmentDataProvider.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomFilterWriterTx);
        when(bloomFilterWriterTx.open()).thenReturn(bloomFilterWriter);
        doNothing().when(segmentDataProvider).invalidate();
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        compactionPolicy = SegmentCompactionPolicyWithManager.from(conf,
                segmentPropertiesManager);
        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                versionController, compactionPolicy);
        final SegmentReplacer<Integer, String> replacer = new SegmentReplacer<>(
                new SegmentFilesRenamer(), deltaCacheController,
                segmentPropertiesManager, segmentFiles);
        subject = new SegmentImpl<>(segmentFiles, conf, versionController,
                segmentPropertiesManager, segmentDataProvider,
                deltaCacheController, segmentSearcher, compactionPolicy,
                compacter, replacer, splitterPolicy);
    }

    @Test
    void createSegment_clones_configuration_and_descriptors() {
        final Directory directory = new MemDirectory();
        final SegmentId originalId = SegmentId.of(10);
        final SegmentId cloneId = SegmentId.of(11);
        final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
        final TypeDescriptorShortString tds = new TypeDescriptorShortString();

        final SegmentImpl<Integer, String> seg = Segment
                .<Integer, String>builder()//
                .withDirectory(directory)//
                .withId(originalId)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
                .withEncodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberWriting(), //
                                new ChunkFilterCrc32Writing(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .withDecodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberValidation(), //
                                new ChunkFilterCrc32Validation(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .build();

        final SegmentImpl<Integer, String> cloned = seg
                .createSegmentWithSameConfig(cloneId);

        assertNotNull(cloned);
        assertEquals(cloneId, cloned.getId());
        // Same directory instance
        assertSame(seg.getSegmentFiles().getDirectoryFacade(),
                cloned.getSegmentFiles().getDirectoryFacade());
        // Same descriptors by type
        assertEquals(seg.getSegmentFiles().getKeyTypeDescriptor().getClass(),
                cloned.getSegmentFiles().getKeyTypeDescriptor().getClass());
        assertEquals(seg.getSegmentFiles().getValueTypeDescriptor().getClass(),
                cloned.getSegmentFiles().getValueTypeDescriptor().getClass());
        // A few key conf properties match (copy constructor used)
        assertEquals(seg.getSegmentConf().getDiskIoBufferSize(),
                cloned.getSegmentConf().getDiskIoBufferSize());
        assertEquals(seg.getSegmentConf().getEncodingChunkFilters().size(),
                cloned.getSegmentConf().getEncodingChunkFilters().size());
        assertEquals(seg.getSegmentConf().getDecodingChunkFilters().size(),
                cloned.getSegmentConf().getDecodingChunkFilters().size());
    }

    @Test
    void getStats_and_getNumberOfKeys_delegate() {
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(stats);
        when(stats.getNumberOfKeys()).thenReturn(7L);
        assertSame(stats, subject.getStats());
        assertEquals(7L, subject.getNumberOfKeys());
    }

    @Test
    void get_uses_searcher_and_provider() {
        when(segmentSearcher.get(eq(123), eq(segmentDataProvider), any()))
                .thenReturn("val");
        assertEquals("val", subject.get(123));
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
    void openDeltaCacheWriter_changes_version_and_returns_writer() {
        try (EntryWriter<Integer, String> writer = subject
                .openDeltaCacheWriter()) {
            assertNotNull(writer);
        }
        verify(versionController).changeVersion();
    }

    @Test
    void openIterator_returns_non_null_and_closes() {
        when(indexIterator.hasNext()).thenReturn(false);
        try (EntryIterator<Integer, String> it = subject.openIterator()) {
            assertNotNull(it);
            assertFalse(it.hasNext());
        }
    }

    @Test
    void policy_exposed() {
        assertSame(splitterPolicy, subject.getSegmentSplitterPolicy());
    }

    @Test
    void identity_and_version() {
        when(versionController.getVersion()).thenReturn(42);
        assertSame(segmentId, subject.getId());
        assertEquals(42, subject.getVersion());
    }

    @Test
    void forceCompact_invokes_compacter_and_commits() {
        when(segmentPropertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of());
        subject.forceCompact();
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
