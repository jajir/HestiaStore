package org.hestiastore.index.segment;

import static org.mockito.Mockito.mock;
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
import org.junit.jupiter.api.Test;

class SegmentFullWriterTxTest {

    @Test
    void commitClearsCachesAndUpdatesStats() {
        @SuppressWarnings("unchecked")
        final SegmentFiles<Integer, String> segmentFiles = (SegmentFiles<Integer, String>) mock(
                SegmentFiles.class);
        @SuppressWarnings("unchecked")
        final ChunkEntryFile<Integer, String> indexFile = (ChunkEntryFile<Integer, String>) mock(
                ChunkEntryFile.class);
        @SuppressWarnings("unchecked")
        final ScarceSegmentIndex<Integer> scarceIndex = (ScarceSegmentIndex<Integer>) mock(
                ScarceSegmentIndex.class);
        @SuppressWarnings("unchecked")
        final ChunkEntryFileWriterTx<Integer, String> chunkWriterTx = (ChunkEntryFileWriterTx<Integer, String>) mock(
                ChunkEntryFileWriterTx.class);
        @SuppressWarnings("unchecked")
        final ScarceIndexWriterTx<Integer> scarceWriterTx = (ScarceIndexWriterTx<Integer>) mock(
                ScarceIndexWriterTx.class);
        @SuppressWarnings("unchecked")
        final ChunkEntryFileWriter<Integer, String> indexWriter = (ChunkEntryFileWriter<Integer, String>) mock(
                ChunkEntryFileWriter.class);
        @SuppressWarnings("unchecked")
        final EntryWriter<Integer, Integer> scarceWriter = (EntryWriter<Integer, Integer>) mock(
                EntryWriter.class);
        @SuppressWarnings("unchecked")
        final SegmentResources<Integer, String> resources = (SegmentResources<Integer, String>) mock(
                SegmentResources.class);
        @SuppressWarnings("unchecked")
        final BloomFilter<Integer> bloomFilter = (BloomFilter<Integer>) mock(
                BloomFilter.class);
        @SuppressWarnings("unchecked")
        final BloomFilterWriterTx<Integer> bloomTx = (BloomFilterWriterTx<Integer>) mock(
                BloomFilterWriterTx.class);
        @SuppressWarnings("unchecked")
        final BloomFilterWriter<Integer> bloomWriter = (BloomFilterWriter<Integer>) mock(
                BloomFilterWriter.class);
        @SuppressWarnings("unchecked")
        final SegmentDeltaCacheController<Integer, String> deltaCacheController = (SegmentDeltaCacheController<Integer, String>) mock(
                SegmentDeltaCacheController.class);
        @SuppressWarnings("unchecked")
        final SegmentCache<Integer, String> segmentCache = (SegmentCache<Integer, String>) mock(
                SegmentCache.class);
        final SegmentPropertiesManager properties = mock(
                SegmentPropertiesManager.class);

        when(segmentFiles.getIndexFile()).thenReturn(indexFile);
        when(segmentFiles.getScarceIndex()).thenReturn(scarceIndex);
        when(indexFile.openWriterTx()).thenReturn(chunkWriterTx);
        when(scarceIndex.openWriterTx()).thenReturn(scarceWriterTx);
        when(chunkWriterTx.openWriter()).thenReturn(indexWriter);
        when(scarceWriterTx.open()).thenReturn(scarceWriter);
        when(resources.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomTx);
        when(bloomTx.open()).thenReturn(bloomWriter);

        final SegmentFullWriterTx<Integer, String> tx = new SegmentFullWriterTx<>(
                segmentFiles, properties, 2, resources, deltaCacheController,
                segmentCache);

        final EntryWriter<Integer, String> writer = tx.open();
        writer.close();
        tx.commit();

        verify(scarceWriterTx).commit();
        verify(chunkWriterTx).commit();
        verify(deltaCacheController).clearPreservingWriteCache();
        verify(properties).setNumberOfKeysInCache(0);
        verify(properties).setNumberOfKeysInIndex(0);
        verify(properties).setNumberOfKeysInScarceIndex(0);
        verify(scarceWriter).close();
        verify(indexWriter).close();
        verify(bloomWriter).close();
        verify(bloomTx).commit();
    }
}
