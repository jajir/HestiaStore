package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.chunkpairfile.ChunkPairFile;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriter;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriterTx;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.scarceindex.ScarceIndex;
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
    private SegmentDataProvider<Integer, String> segmentDataProvider;
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
    private BloomFilterWriter<Integer> bloomFilterWriter;
    @Mock
    private ScarceIndex<Integer> scarceIndex;
    @Mock
    private ChunkPairFile<Integer, String> chunkPairFile;
    @Mock
    private ChunkPairFileWriterTx<Integer, String> chunkPairWriterTx;
    @Mock
    private ScarceIndexWriterTx<Integer> scarceWriterTx;
    @Mock
    private PairWriter<Integer, Integer> scarcePairWriter;
    @Mock
    private ChunkPairFileWriter<Integer, String> chunkPairWriter;
    @Mock
    private PairIteratorWithCurrent<Integer, String> indexIterator;

    private SegmentConf conf;
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private SegmentImpl<Integer, String> subject;

    @BeforeEach
    void setUpSubject() {
        conf = new SegmentConf(100L, 200L, 3, 0, 0, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        when(segmentFiles.getId()).thenReturn(segmentId);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(tds);
        when(segmentFiles.getIndexFile()).thenReturn(chunkPairFile);
        when(chunkPairFile.openWriterTx()).thenReturn(chunkPairWriterTx);
        when(chunkPairWriterTx.openWriter()).thenReturn(chunkPairWriter);
        when(chunkPairFile.openIterator()).thenReturn(indexIterator);
        when(segmentFiles.getScarceIndex()).thenReturn(scarceIndex);
        when(scarceIndex.openWriterTx()).thenReturn(scarceWriterTx);
        when(scarceWriterTx.openWriter()).thenReturn(scarcePairWriter);
        doNothing().when(scarceIndex).loadCache();
        when(deltaCacheController.getDeltaCache()).thenReturn(deltaCache);
        when(deltaCache.getAsSortedList()).thenReturn(List.of());
        when(indexIterator.hasNext()).thenReturn(false);
        when(segmentDataProvider.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriter()).thenReturn(bloomFilterWriter);
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
        assertSame(seg.getSegmentFiles().getDirectory(),
                cloned.getSegmentFiles().getDirectory());
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
        when(segmentSearcher.get(123, deltaCache, bloomFilter, scarceIndex))
                .thenReturn("val");
        assertEquals("val", subject.get(123));
        verify(segmentSearcher).get(123, deltaCache, bloomFilter, scarceIndex);
    }

    @Test
    void openDeltaCacheWriter_changes_version_and_returns_writer() {
        try (PairWriter<Integer, String> writer = subject
                .openDeltaCacheWriter()) {
            assertNotNull(writer);
        }
        verify(versionController).changeVersion();
    }

    @Test
    void openIterator_returns_non_null_and_closes() {
        when(indexIterator.hasNext()).thenReturn(false);
        try (PairIterator<Integer, String> it = subject.openIterator()) {
            assertNotNull(it);
            assertFalse(it.hasNext());
        }
    }

    @Test
    void splitter_and_policy_exposed() {
        assertNotNull(subject.getSegmentSplitter());
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
        verify(chunkPairWriterTx).commit();
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
        when(indexIterator.next()).thenReturn(Pair.of(1, "a"))
                .thenReturn(Pair.of(3, "b")).thenReturn(Pair.of(2, "c"));
        assertThrows(org.hestiastore.index.IndexException.class,
                () -> subject.checkAndRepairConsistency());
    }
}
