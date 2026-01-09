package org.hestiastore.index.segment;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.bloomfilter.BloomFilterWriterTx;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriterTx;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentFullWriterTxTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private ChunkEntryFile<Integer, String> indexFile;
    @Mock
    private ScarceSegmentIndex<Integer> scarceIndex;
    @Mock
    private ChunkEntryFileWriterTx<Integer, String> chunkWriterTx;
    @Mock
    private ScarceIndexWriterTx<Integer> scarceWriterTx;
    @Mock
    private ChunkEntryFileWriter<Integer, String> indexWriter;
    @Mock
    private EntryWriter<Integer, Integer> scarceWriter;
    @Mock
    private SegmentResources<Integer, String> resources;
    @Mock
    private BloomFilter<Integer> bloomFilter;
    @Mock
    private BloomFilterWriterTx<Integer> bloomTx;
    @Mock
    private BloomFilterWriter<Integer> bloomWriter;
    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;
    @Mock
    private SegmentCache<Integer, String> segmentCache;
    @Mock
    private SegmentPropertiesManager properties;

    private SegmentFullWriterTx<Integer, String> subject;

    @BeforeEach
    void setUp() {
        when(segmentFiles.getIndexFile()).thenReturn(indexFile);
        when(segmentFiles.getScarceIndex()).thenReturn(scarceIndex);
        when(indexFile.openWriterTx()).thenReturn(chunkWriterTx);
        when(scarceIndex.openWriterTx()).thenReturn(scarceWriterTx);
        when(chunkWriterTx.openWriter()).thenReturn(indexWriter);
        when(scarceWriterTx.open()).thenReturn(scarceWriter);
        when(resources.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomTx);
        when(bloomTx.open()).thenReturn(bloomWriter);
        subject = new SegmentFullWriterTx<>(segmentFiles, properties, 2,
                resources, deltaCacheController, segmentCache);
    }

    @AfterEach
    void tearDown() {
        subject = null;
    }

    @Test
    void commitClearsCachesAndUpdatesStats() {
        try (EntryWriter<Integer, String> writer = subject.open()) {
            // no-op
        }
        subject.commit();

        verify(scarceWriterTx).commit();
        verify(chunkWriterTx).commit();
        verify(bloomTx).commit();
        verify(deltaCacheController).clearPreservingWriteCache();
        verify(properties).setNumberOfKeysInCache(0);
        verify(properties).setNumberOfKeysInIndex(0);
        verify(properties).setNumberOfKeysInScarceIndex(0);
        verify(scarceWriter).close();
        verify(indexWriter).close();
        verify(bloomWriter).close();
    }
}
