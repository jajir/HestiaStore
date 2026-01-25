package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Executor;

import org.hestiastore.index.IndexException;
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
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
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
    private BloomFilter<Integer> bloomFilter;
    @Mock
    private Directory directory;
    @Mock
    private FileReaderSeekable seekableReader;
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
    private SegmentCore<Integer, String> core;

    @BeforeEach
    void setUpSubject() {
        conf = new SegmentConf(50, 100, 1000, 3, 7,
                SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        when(segmentFiles.getId()).thenReturn(segmentId);
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(tds);
        when(segmentFiles.getIndexFile()).thenReturn(chunkPairFile);
        when(segmentFiles.getIndexFileName()).thenReturn("segment.index");
        when(segmentFiles.getBloomFilterFileName()).thenReturn("segment.bloom");
        when(segmentFiles.getAsyncDirectory()).thenReturn(
                AsyncDirectoryAdapter.wrap(directory));
        when(segmentFiles.copyWithVersion(anyLong())).thenReturn(segmentFiles);
        when(directory.isFileExists("segment.index")).thenReturn(true);
        when(directory.getFileReaderSeekable("segment.index"))
                .thenReturn(seekableReader);
        when(chunkPairFile.openWriterTx()).thenReturn(chunkEntryWriterTx);
        when(chunkEntryWriterTx.openWriter()).thenReturn(chunkEntryWriter);
        when(chunkEntryWriter.flush()).thenReturn(
                CellPosition.of(DataBlockSize.ofDataBlockSize(1024), 0));
        when(chunkPairFile.openIterator()).thenReturn(indexIterator);
        when(segmentFiles.getScarceIndex()).thenReturn(scarceIndex);
        when(scarceIndex.openWriterTx()).thenReturn(scarceWriterTx);
        when(scarceWriterTx.open()).thenReturn(scarceEntryWriter);
        doNothing().when(scarceIndex).loadCache();
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(indexIterator.hasNext()).thenReturn(false);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomFilterWriterTx);
        when(bloomFilterWriterTx.open()).thenReturn(bloomFilterWriter);
        doNothing().when(segmentDataProvider).invalidate();
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                versionController);
        core = createCore(versionController);
        final SegmentMaintenancePolicyThreshold<Integer, String> maintenancePolicy = new SegmentMaintenancePolicyThreshold<>(
                conf.getMaxNumberOfKeysInSegmentCache(),
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfDeltaCacheFiles());
        subject = new SegmentImpl<>(core, compacter, Runnable::run,
                maintenancePolicy);
    }

    @AfterEach
    void tearDown() {
        if (subject != null) {
            if (subject.getState() != SegmentState.ERROR) {
                closeAndAwait(subject);
            }
        }
    }

    @Test
    void getStats_and_getNumberOfKeys_delegate() {
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(stats);
        when(stats.getNumberOfKeys()).thenReturn(7L);
        assertSame(stats, subject.getStats());
        assertEquals(7L, subject.getNumberOfKeys());
    }

    @Test
    void getMaxNumberOfDeltaCacheFiles_delegates_to_properties() {
        when(segmentPropertiesManager.getDeltaFileCount()).thenReturn(4);

        assertEquals(4, subject.getMaxNumberOfDeltaCacheFiles());
    }

    @Test
    void constructor_requires_maintenance_executor() {
        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                versionController);
        final SegmentMaintenancePolicyThreshold<Integer, String> maintenancePolicy = new SegmentMaintenancePolicyThreshold<>(
                conf.getMaxNumberOfKeysInSegmentCache(),
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfDeltaCacheFiles());

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new SegmentImpl<>(core, compacter, null,
                        maintenancePolicy));

        assertEquals("Property 'maintenanceExecutor' must not be null.",
                e.getMessage());
    }

    @Test
    void put_writes_to_segment_cache() {
        assertEquals(SegmentResultStatus.OK,
                subject.put(1, "A").getStatus());
        assertEquals(SegmentResultStatus.OK,
                subject.put(2, "B").getStatus());

        final SegmentResult<String> first = subject.get(1);
        final SegmentResult<String> second = subject.get(2);
        assertEquals(SegmentResultStatus.OK, first.getStatus());
        assertEquals(SegmentResultStatus.OK, second.getStatus());
        assertEquals("A", first.getValue());
        assertEquals("B", second.getValue());
        assertEquals(2, subject.getNumberOfKeysInWriteCache());
        verify(versionController, never()).changeVersion();
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
    void put_triggers_flush_when_policy_requests() {
        final SegmentImpl<Integer, String> segment = spy(
                newSegmentWithPolicy(SegmentMaintenanceDecision.flushOnly()));
        try {
            doReturn(SegmentResult.ok()).when(segment).flush();

            final SegmentResult<Void> result = segment.put(1, "A");

            assertEquals(SegmentResultStatus.OK, result.getStatus());
            verify(segment).flush();
            verify(segment, never()).compact();
        } finally {
            closeAndAwait(segment);
        }
    }

    @Test
    void put_triggers_compact_when_policy_requests() {
        final SegmentImpl<Integer, String> segment = spy(
                newSegmentWithPolicy(SegmentMaintenanceDecision.compactOnly()));
        try {
            doReturn(SegmentResult.ok()).when(segment).compact();

            final SegmentResult<Void> result = segment.put(1, "A");

            assertEquals(SegmentResultStatus.OK, result.getStatus());
            verify(segment).compact();
            verify(segment, never()).flush();
        } finally {
            closeAndAwait(segment);
        }
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
    void flush_changes_version_when_entries_present() {
        assertEquals(SegmentResultStatus.OK,
                subject.put(1, "A").getStatus());
        reset(versionController);
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
    void flush_transitions_through_maintenance_and_ready() {
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            final SegmentResult<Void> result = segment.flush();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertTrue(executor.hasTask());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void flush_allows_writes_during_maintenance_and_preserves_snapshot() {
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "A").getStatus());

            final SegmentResult<Void> result = segment.flush();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "B").getStatus());

            assertTrue(executor.hasTask());
            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
            assertEquals(1, segment.getNumberOfKeysInWriteCache());
            assertEquals("A", segment.get(1).getValue());
            assertEquals("B", segment.get(2).getValue());
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void flush_returns_busy_when_maintenance_running() {
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            final SegmentResult<Void> first = segment.flush();
            final SegmentResult<Void> second = segment.flush();

            assertEquals(SegmentResultStatus.OK, first.getStatus());
            assertEquals(SegmentResultStatus.BUSY, second.getStatus());

            assertTrue(executor.hasTask());
            executor.runTask();
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void flush_returns_closed_when_closed() {
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        segment.close();
        assertTrue(executor.hasTask());
        executor.runTask();

        final SegmentResult<Void> result = segment.flush();

        assertEquals(SegmentResultStatus.CLOSED, result.getStatus());
    }

    @Test
    void compact_allows_reads_during_maintenance() {
        when(segmentPropertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of());
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "A").getStatus());

            final SegmentResult<Void> result = segment.compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());

            final SegmentResult<String> read = segment.get(1);
            assertEquals(SegmentResultStatus.OK, read.getStatus());
            assertEquals("A", read.getValue());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void compact_returns_busy_when_flush_running() {
        when(segmentPropertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of());
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            final SegmentResult<Void> flushResult = segment.flush();
            assertEquals(SegmentResultStatus.OK, flushResult.getStatus());

            final SegmentResult<Void> compactResult = segment.compact();

            assertEquals(SegmentResultStatus.BUSY, compactResult.getStatus());

            assertTrue(executor.hasTask());
            executor.runTask();
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void compact_transitions_through_maintenance_and_ready() {
        when(segmentPropertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of());
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            final SegmentResult<Void> result = segment.compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertTrue(executor.hasTask());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void compact_returns_busy_when_maintenance_running() {
        when(segmentPropertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of());
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            final SegmentResult<Void> first = segment.compact();
            final SegmentResult<Void> second = segment.compact();

            assertEquals(SegmentResultStatus.OK, first.getStatus());
            assertEquals(SegmentResultStatus.BUSY, second.getStatus());

            assertTrue(executor.hasTask());
            executor.runTask();
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void flush_failure_sets_error_state() {
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "A").getStatus());
            when(deltaCacheController.openWriter())
                    .thenThrow(new RuntimeException("flush failed"));

            final SegmentResult<Void> result = segment.flush();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertTrue(executor.hasTask());
            executor.runTask();

            assertEquals(SegmentState.ERROR, segment.getState());
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void compact_failure_sets_error_state() {
        final CapturingExecutor executor = new CapturingExecutor();
        doThrow(new RuntimeException("compact failed")).when(versionController)
                .changeVersion();
        final SegmentImpl<Integer, String> segment = newSegment(executor,
                versionController);
        try {
            final SegmentResult<Void> result = segment.compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertTrue(executor.hasTask());
            executor.runTask();

            assertEquals(SegmentState.ERROR, segment.getState());
        } finally {
            segment.close();
            executor.runTask();
        }
    }

    @Test
    void openIterator_returns_error_when_iterator_fails() {
        when(chunkPairFile.openIterator())
                .thenThrow(new RuntimeException("boom"));
        final SegmentResult<EntryIterator<Integer, String>> result = subject
                .openIterator();

        assertEquals(SegmentResultStatus.ERROR, result.getStatus());
        assertEquals(SegmentState.ERROR, subject.getState());
    }

    @Test
    void close_returns_busy_for_full_isolation_iterator() throws Exception {
        final SegmentImpl<Integer, String> segment = newSegment(Runnable::run,
                versionController);
        final SegmentResult<EntryIterator<Integer, String>> iteratorResult = segment
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);

        assertEquals(SegmentResultStatus.OK, iteratorResult.getStatus());
        assertNotNull(iteratorResult.getValue());

        final SegmentResult<Void> firstClose = segment.close();
        assertEquals(SegmentResultStatus.BUSY, firstClose.getStatus());

        iteratorResult.getValue().close();
        final SegmentResult<Void> secondClose = segment.close();
        assertEquals(SegmentResultStatus.OK, secondClose.getStatus());

        assertEquals(SegmentState.CLOSED, segment.getState());
    }

    @Test
    void maintenance_keeps_closed_state_after_completion() throws Exception {
        final CapturingExecutor executor = new CapturingExecutor();
        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                versionController);
        final SegmentMaintenancePolicyThreshold<Integer, String> maintenancePolicy = new SegmentMaintenancePolicyThreshold<>(
                conf.getMaxNumberOfKeysInSegmentCache(),
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfDeltaCacheFiles());
        final SegmentImpl<Integer, String> segment = new SegmentImpl<>(
                core, compacter, executor, maintenancePolicy);

        final SegmentResult<Void> result = segment.flush();
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertTrue(executor.hasTask());

        final SegmentResult<Void> closing = segment.close();
        assertEquals(SegmentResultStatus.BUSY, closing.getStatus());

        executor.runTask();

        final SegmentResult<Void> secondClose = segment.close();
        assertEquals(SegmentResultStatus.OK, secondClose.getStatus());
        assertTrue(executor.hasTask());
        executor.runTask();

        assertEquals(SegmentState.CLOSED, segment.getState());
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
        assertThrows(IndexException.class,
                () -> subject.checkAndRepairConsistency());
    }

    private static final class CapturingExecutor implements Executor {

        private Runnable task;

        @Override
        public void execute(final Runnable command) {
            this.task = command;
        }

        boolean hasTask() {
            return task != null;
        }

        void runTask() {
            if (task != null) {
                final Runnable toRun = task;
                task = null;
                toRun.run();
            }
        }
    }

    private SegmentImpl<Integer, String> newSegment(final Executor executor,
            final VersionController controller) {
        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                controller);
        final SegmentCore<Integer, String> localCore = createCore(controller);
        final SegmentMaintenancePolicyThreshold<Integer, String> maintenancePolicy = new SegmentMaintenancePolicyThreshold<>(
                conf.getMaxNumberOfKeysInSegmentCache(),
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfDeltaCacheFiles());
        return new SegmentImpl<>(localCore, compacter, executor,
                maintenancePolicy);
    }

    private SegmentCore<Integer, String> createCore(
            final VersionController controller) {
        final SegmentCache<Integer, String> segmentCache = new SegmentCache<>(
                tdi.getComparator(), tds, List.of(),
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                conf.getMaxNumberOfKeysInSegmentCache());
        deltaCacheController.setSegmentCache(segmentCache);
        final SegmentReadPath<Integer, String> readPath = new SegmentReadPath<>(
                segmentFiles, conf, segmentDataProvider, segmentSearcher,
                segmentCache, controller);
        final SegmentWritePath<Integer, String> writePath = new SegmentWritePath<>(
                segmentCache, controller);
        final SegmentMaintenancePath<Integer, String> maintenancePath = new SegmentMaintenancePath<>(
                segmentFiles, conf, segmentPropertiesManager, segmentDataProvider,
                deltaCacheController);
        return new SegmentCore<>(segmentFiles, controller,
                segmentPropertiesManager, segmentCache, readPath, writePath,
                maintenancePath);
    }

    private SegmentImpl<Integer, String> newSegmentWithPolicy(
            final SegmentMaintenanceDecision decision) {
        final SegmentMaintenancePolicy<Integer, String> policy = segment -> decision;
        final SegmentCompacter<Integer, String> compacter = new SegmentCompacter<>(
                versionController);
        return new SegmentImpl<>(core, compacter, Runnable::run, policy);
    }
}
