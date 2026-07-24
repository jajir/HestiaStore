package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileReaderSeekableSupplier;
import org.junit.jupiter.api.AfterEach;
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
    private SegmentResources<Integer> segmentResources;
    @Mock
    private SegmentSearcher<Integer, String> segmentSearcher;
    @Mock
    private SegmentCache<Integer, String> segmentCache;
    @Mock
    private VersionController versionController;
    @Mock
    private ChunkEntryFile<Integer, String> chunkEntryFile;
    @Mock
    private Directory asyncDirectory;
    @Mock
    private EntryIteratorWithCurrent<Integer, String> baseIterator;
    @Mock
    private FileReaderSeekable seekableReader;
    @Mock
    private FileReaderSeekableSupplier firstSeekableReaderSupplier;
    @Mock
    private FileReaderSeekableSupplier secondSeekableReaderSupplier;

    private SegmentReadPath<Integer, String> subject;
    private final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();

    @BeforeEach
    void setUp() {
        final SegmentConf conf = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(1)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(1)
                .withMaxNumberOfKeysInSegmentCache(1)
                .withMaxNumberOfKeysInChunk(1)
                .withMaxNumberOfDeltaCacheFiles(1)
                .withBloomFilterNumberOfHashFunctions(0)
                .withBloomFilterIndexSizeInBytes(0)
                .withBloomFilterProbabilityOfFalsePositive(0.01)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();
        when(segmentFiles.getIndexFile()).thenReturn(chunkEntryFile);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(keyDescriptor);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(valueDescriptor);
        when(segmentFiles.getId()).thenReturn(SegmentId.of(1));
        when(segmentFiles.getIndexFileName()).thenReturn("segment.index");
        when(segmentFiles.getDirectory()).thenReturn(asyncDirectory);
        when(asyncDirectory.isFileExists("segment.index")).thenReturn(false);
        when(asyncDirectory.getFileReaderSeekable("segment.index"))
                .thenReturn(seekableReader);
        when(asyncDirectory.getFileReaderSeekableSupplier("segment.index"))
                .thenReturn(() -> seekableReader);
        when(segmentCache.getAsSortedList()).thenReturn(List.of());
        when(segmentCache.mergedIterator())
                .thenReturn(List.<Entry<Integer, String>>of().iterator());
        when(chunkEntryFile.openIterator()).thenReturn(baseIterator);
        when(baseIterator.hasNext()).thenReturn(false);
        subject = new SegmentReadPath<>(segmentFiles, conf, segmentResources,
                segmentSearcher, segmentCache, versionController);
    }

    @AfterEach
    void tearDown() {
        subject.close();
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
        when(segmentSearcher.get(intThat(key -> key == 1),
                same(segmentResources), any()))
                .thenReturn("value");

        assertEquals("value", subject.get(1));

        verify(segmentSearcher).get(intThat(key -> key == 1),
                same(segmentResources), any());
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

    @Test
    void getSegmentIndexSearcher_closes_concurrent_cas_loser()
            throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch suppliersRequested = new CountDownLatch(2);
        final CountDownLatch allowConstruction = new CountDownLatch(1);
        final AtomicInteger supplierIndex = new AtomicInteger();
        final AtomicInteger closedSupplierCount = new AtomicInteger();
        doAnswer(invocation -> closedSupplierCount.incrementAndGet())
                .when(firstSeekableReaderSupplier).close();
        doAnswer(invocation -> closedSupplierCount.incrementAndGet())
                .when(secondSeekableReaderSupplier).close();
        when(asyncDirectory.getFileReaderSeekableSupplier("segment.index"))
                .thenAnswer(invocation -> {
                    final int index = supplierIndex.getAndIncrement();
                    suppliersRequested.countDown();
                    allowConstruction.await();
                    return index == 0 ? firstSeekableReaderSupplier
                            : secondSeekableReaderSupplier;
                });
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<SegmentIndexSearcher<Integer, String>> firstFuture = executor
                    .submit(subject::getSegmentIndexSearcher);
            final Future<SegmentIndexSearcher<Integer, String>> secondFuture = executor
                    .submit(subject::getSegmentIndexSearcher);
            assertTrue(suppliersRequested.await(5, TimeUnit.SECONDS));
            allowConstruction.countDown();

            assertSame(firstFuture.get(5, TimeUnit.SECONDS),
                    secondFuture.get(5, TimeUnit.SECONDS));
            assertEquals(2, supplierIndex.get());
            assertEquals(1, closedSupplierCount.get());

            subject.close();

            assertEquals(2, closedSupplierCount.get());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            allowConstruction.countDown();
            executor.shutdownNow();
        }
    }
}
