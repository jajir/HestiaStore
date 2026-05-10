package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexImplTest {

    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = IndexInternalConcurrent.createStarted(
                new MemDirectory(),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), effective(conf),
                ExecutorRegistryFixture.from(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void putGetAndDeleteRoundTrip() {
        index.put(1, "one");

        assertEquals("one", index.get(1));

        index.delete(1);
        assertNull(index.get(1));
    }

    @Test
    void openingFactoryDefersStartupCompletion() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        try (IndexInternalConcurrent<Integer, String> openingIndex =
                IndexInternalConcurrent.createOpening(new MemDirectory(),
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf),
                        ExecutorRegistryFixture.from(conf))) {
            assertEquals(SegmentIndexState.OPENING,
                    openingIndex.runtimeMonitoring().snapshot().getState());

            openingIndex.completeStartup();
            openingIndex.completeStartup();

            assertEquals(SegmentIndexState.READY,
                    openingIndex.runtimeMonitoring().snapshot().getState());
        }
    }

    @Test
    void failedStartupCleanupClosesOpeningIndexWithoutErrorState() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        final IndexInternalConcurrent<Integer, String> openingIndex =
                IndexInternalConcurrent.createOpening(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf),
                        ExecutorRegistryFixture.from(conf));
        final SegmentIndexStateMachine stateMachine =
                SegmentIndexTestAccess.stateMachine(openingIndex);

        try {
            openingIndex.abortStartup(
                    new IllegalStateException("startup failed"));
            openingIndex.close();

            assertEquals(SegmentIndexState.CLOSED, stateMachine.getState());
            assertFalse(directory.isFileExists(".lock"));
        } finally {
            cleanupOpeningIndex(openingIndex);
        }
    }

    @Test
    void failedStartupCleanupClosesReadyIndexWithoutErrorState() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        final IndexInternalConcurrent<Integer, String> openingIndex =
                IndexInternalConcurrent.createOpening(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf),
                        ExecutorRegistryFixture.from(conf));
        final SegmentIndexStateMachine stateMachine =
                SegmentIndexTestAccess.stateMachine(openingIndex);

        try {
            openingIndex.completeStartup();
            openingIndex.abortStartup(
                    new IllegalStateException("startup failed after ready"));
            openingIndex.close();

            assertEquals(SegmentIndexState.CLOSED, stateMachine.getState());
            assertFalse(directory.isFileExists(".lock"));
        } finally {
            cleanupOpeningIndex(openingIndex);
        }
    }

    @Test
    void closeReleasesDirectoryLock() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        final IndexInternalConcurrent<Integer, String> lockedIndex =
                IndexInternalConcurrent.createStarted(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf),
                        ExecutorRegistryFixture.from(conf));

        lockedIndex.close();

        assertFalse(directory.isFileExists(".lock"));
    }

    @Test
    void failedOpenReleasesDirectoryLock() {
        final Directory directory = mock(Directory.class);
        final FileLock fileLock = mock(FileLock.class);
        final IndexConfiguration<Integer, String> conf = buildConf();
        when(directory.isFileExists(".lock")).thenReturn(false);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(false, true);
        when(directory.openSubDirectory(anyString()))
                .thenThrow(new IllegalStateException("open failed"));

        assertThrows(RuntimeException.class,
                () -> IndexInternalConcurrent.createStarted(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf),
                        ExecutorRegistryFixture.from(conf)));

        verify(fileLock).unlock();
    }

    private void cleanupOpeningIndex(
            final IndexInternalConcurrent<Integer, String> openingIndex) {
        if (openingIndex.wasClosed()) {
            return;
        }
        openingIndex.abortStartup(new IllegalStateException("test cleanup"));
        openingIndex.close();
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("segment-index-impl-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

}
