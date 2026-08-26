package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuWritingRuntimeFailureTest {

    @Spy
    private MemDirectory directory;

    @Mock
    private FileLock fileLock;

    @Mock
    private SenkuIngestor<Integer, Long> ingestor;

    @Mock
    private SenkuMaintenanceCoordinator<Integer, Long> coordinator;

    private AtomicReference<IndexException> firstFailure;
    private ScheduledExecutorService controlExecutor;
    private ThreadPoolExecutor workerExecutor;
    private SenkuWritingRuntime<Integer, Long> writing;

    @BeforeEach
    void setUp() {
        firstFailure = new AtomicReference<>();
        controlExecutor = Executors.newSingleThreadScheduledExecutor();
        workerExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
        writing = new SenkuWritingRuntime<>(directory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(),
                DataBlockSize.ofDataBlockSize(1_024), 1, fileLock,
                new ReentrantLock(), ingestor, coordinator, controlExecutor,
                workerExecutor, firstFailure);
    }

    @AfterEach
    void tearDown() {
        controlExecutor.shutdownNow();
        workerExecutor.shutdownNow();
    }

    @Test
    void finishRethrowsCallerFlushFailure() {
        final IllegalStateException cause = new IllegalStateException(
                "storage write failed");
        final IndexException expected = new IndexException(
                "Unable to flush caller entries.", cause);
        doThrow(expected).when(ingestor).stopAcceptingAndFlush();

        final IndexException thrown = assertThrows(IndexException.class,
                writing::finishWriting);

        assertSame(expected, thrown);
        assertSame(cause, thrown.getCause());
    }

    @Test
    void completedResultRethrowsRecordedBackgroundFailure() {
        final IllegalStateException cause = new IllegalStateException(
                "merge failed");
        final IndexException expected = new IndexException(
                "Senku maintenance failed.", cause);
        firstFailure.set(expected);

        final IndexException thrown = assertThrows(IndexException.class,
                writing::completedResult);

        assertSame(expected, thrown);
        assertSame(cause, thrown.getCause());
    }

    @Test
    void finishPreservesPublicationAndCleanupFailures() {
        final IllegalStateException cause = new IllegalStateException(
                "ready rename failed");
        final IndexException publicationFailure = new IndexException(
                "Unable to publish ready metadata.", cause);
        final IndexException cleanupFailure = new IndexException(
                "Unable to release lock.");
        doThrow(publicationFailure).when(directory).renameFile(
                SenkuFileNames.temporary(SenkuFileNames.READY_FILE),
                SenkuFileNames.READY_FILE);
        doThrow(cleanupFailure).when(fileLock).unlock();
        when(coordinator.isDrainComplete()).thenReturn(true);

        final IndexException thrown = assertThrows(IndexException.class,
                writing::finishWriting);

        assertSame(publicationFailure, thrown);
        assertSame(cause, thrown.getCause());
        assertSame(cleanupFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void completedResultUsesFallbackWhenNoOutcomeWasRecorded() {
        final IndexException thrown = assertThrows(IndexException.class,
                writing::completedResult);

        assertEquals("Unable to finish Senku writing.", thrown.getMessage());
    }
}
