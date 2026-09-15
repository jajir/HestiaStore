package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuFailureShutdownIT {

    @Mock
    private SenkuFlushWriter<Integer, Long> flushWriter;

    @Mock
    private SenkuMaintenanceCoordinator<Integer, Long> coordinator;

    private MemDirectory directory;
    private CountDownLatch flushStarted;
    private CountDownLatch releaseFlush;
    private ExecutorService callers;
    private ScheduledExecutorService controlExecutor;
    private ThreadPoolExecutor workerExecutor;
    private AtomicReference<IndexException> firstFailure;
    private SenkuWritingRuntime<Integer, Long> writing;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        flushStarted = new CountDownLatch(1);
        releaseFlush = new CountDownLatch(1);
        callers = Executors.newSingleThreadExecutor();
        controlExecutor = Executors.newSingleThreadScheduledExecutor();
        workerExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
        firstFailure = new AtomicReference<>();
        final FileLock fileLock = directory.getLock(SenkuFileNames.LOCK_FILE);
        fileLock.lock();
        final SenkuIngestor<Integer, Long> ingestor = new SenkuIngestor<>(
                new ReentrantLock(), (key, first, second) -> first + second,
                key -> key, flushWriter, 2, 2);
        writing = new SenkuWritingRuntime<>(directory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(),
                DataBlockSize.ofDataBlockSize(1_024), 1, fileLock,
                new ReentrantLock(), ingestor, coordinator, controlExecutor,
                workerExecutor, firstFailure);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        releaseFlush.countDown();
        if (!controlExecutor.isShutdown()) {
            final IndexException cleanupFailure = new IndexException(
                    "Stopping unfinished failure-shutdown test.");
            firstFailure.compareAndSet(null, cleanupFailure);
            writing.backgroundFailure(cleanupFailure);
        }
        callers.shutdownNow();
        assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(controlExecutor.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(workerExecutor.isTerminated());
    }

    @ParameterizedTest
    @CsvSource({ "false, false", "false, true", "true, false", "true, true" })
    void failureRetainsRootLockUntilCallerFlushReleasesStorage(
            final boolean finishing, final boolean flushFails) throws Exception {
        final IndexException backgroundFailure = new IndexException(
                "Background maintenance failed.");
        doAnswer(invocation -> {
            flushStarted.countDown();
            assertTrue(releaseFlush.await(5, TimeUnit.SECONDS));
            if (flushFails) {
                throw new IllegalStateException("Caller flush also failed.");
            }
            return null;
        }).when(flushWriter).write(anyLong(), anyList());
        writing.put(1, 1L);
        final Future<?> operation = callers.submit(() -> {
            if (finishing) {
                writing.finishWriting();
            } else {
                writing.put(2, 2L);
            }
        });
        assertTrue(flushStarted.await(5, TimeUnit.SECONDS));

        firstFailure.set(backgroundFailure);
        // A control tick can observe the recorded failure before its reporter
        // acquires the lifecycle lock. Exercise that order directly, including
        // finishWriting holding the lifecycle lock across the final flush.
        writing.completionProcessed();

        assertFalse(controlExecutor.awaitTermination(100,
                TimeUnit.MILLISECONDS));
        assertTrue(directory.isFileExists(SenkuFileNames.LOCK_FILE));
        final FileLock secondLock = directory
                .getLock(SenkuFileNames.LOCK_FILE);
        assertThrows(IndexException.class, secondLock::lock);
        releaseFlush.countDown();
        if (finishing || flushFails) {
            assertThrows(ExecutionException.class,
                    () -> operation.get(5, TimeUnit.SECONDS));
        } else {
            operation.get(5, TimeUnit.SECONDS);
        }

        assertTrue(controlExecutor.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(workerExecutor.isTerminated());
        assertFalse(directory.isFileExists(SenkuFileNames.LOCK_FILE));
        assertSame(backgroundFailure, firstFailure.get());
        assertSame(backgroundFailure, assertThrows(IndexException.class,
                writing::completedResult));
    }
}
