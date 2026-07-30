package org.hestiastore.index.segmentindex.wal;

import static org.hestiastore.index.segmentindex.wal.WalRuntimeTestSupport.openWithStorageAndQueue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.WalDurabilityMode;
import org.junit.jupiter.api.Test;

class WalRuntimeConcurrencyTest {

    private static final TypeDescriptorString STRING_DESCRIPTOR = new TypeDescriptorString();

    @Test
    void syncModeSharesOnePhysicalSyncAcrossOneWorkerBatch() throws Exception {
        final int writerCount = 16;
        final GatedWalAppendQueue<String, String> queue = new GatedWalAppendQueue<>(writerCount, writerCount);
        final ExecutorService executor = Executors
                .newFixedThreadPool(writerCount);
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC).build();

        try (WalRuntime<String, String> runtime = openWithStorageAndQueue(wal,
                new WalStorageMem(new MemDirectory()), STRING_DESCRIPTOR,
                STRING_DESCRIPTOR, queue)) {
            final List<Future<Long>> writes = new ArrayList<>(writerCount);
            for (int i = 0; i < writerCount; i++) {
                final int writer = i;
                writes.add(executor.submit(() -> runtime.appendPut(
                        "key-" + writer, "value-" + writer)));
            }
            assertTrue(queue.awaitAccepted(5, TimeUnit.SECONDS));
            queue.releaseWorker();

            final Set<Long> lsns = new HashSet<>();
            for (final Future<Long> write : writes) {
                assertTrue(lsns.add(write.get(5, TimeUnit.SECONDS)));
            }
            final WalMonitoring snapshot = runtime.statsSnapshot();
            assertEquals(writerCount, snapshot.appendCount());
            assertEquals(writerCount, snapshot.durableLsn());
            assertEquals(1L, snapshot.syncCount());
            assertEquals(snapshot.appendBytes(),
                    snapshot.syncBatchBytesTotal());
            assertEquals(snapshot.appendBytes(),
                    snapshot.syncBatchBytesMax());
        } finally {
            queue.releaseWorker();
            executor.shutdownNow();
        }
    }

    @Test
    void closeDrainsAcceptedAppendsBeforeStopMarker() throws Exception {
        final GatedWalAppendQueue<String, String> queue = new GatedWalAppendQueue<>(2, 2);
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.ASYNC).build();
        final WalRuntime<String, String> runtime = openWithStorageAndQueue(wal,
                new WalStorageMem(new MemDirectory()), STRING_DESCRIPTOR,
                STRING_DESCRIPTOR, queue);

        try {
            final Future<Long> first = executor
                    .submit(() -> runtime.appendPut("first", "value"));
            final Future<Long> second = executor
                    .submit(() -> runtime.appendPut("second", "value"));
            assertTrue(queue.awaitAccepted(5, TimeUnit.SECONDS));
            final Future<?> close = executor.submit(() -> {
                closeStarted.countDown();
                runtime.close();
            });
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertFalse(close.isDone());

            queue.releaseWorker();
            final Set<Long> lsns = Set.of(first.get(5, TimeUnit.SECONDS),
                    second.get(5, TimeUnit.SECONDS));
            close.get(5, TimeUnit.SECONDS);

            assertEquals(Set.of(1L, 2L), lsns);
        } finally {
            queue.releaseWorker();
            runtime.close();
            executor.shutdownNow();
        }
    }

    @Test
    void fullQueueOfferRemainsInterruptible() throws Exception {
        final GatedWalAppendQueue<String, String> queue = new GatedWalAppendQueue<>(1, 2);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        final AtomicBoolean interruptRestored = new AtomicBoolean(false);
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.ASYNC).build();
        final WalRuntime<String, String> runtime = openWithStorageAndQueue(wal,
                new WalStorageMem(new MemDirectory()), STRING_DESCRIPTOR,
                STRING_DESCRIPTOR, queue);

        try {
            final Future<Long> first = executor
                    .submit(() -> runtime.appendPut("first", "value"));
            final Thread blockedWriter = new Thread(() -> {
                try {
                    runtime.appendPut("blocked", "value");
                } catch (final RuntimeException ex) {
                    failure.set(ex);
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                }
            });
            blockedWriter.start();
            assertTrue(queue.awaitOfferAttempts(5, TimeUnit.SECONDS));

            blockedWriter.interrupt();
            blockedWriter.join(TimeUnit.SECONDS.toMillis(5));

            assertFalse(blockedWriter.isAlive());
            assertInstanceOf(IndexException.class, failure.get());
            assertTrue(interruptRestored.get());
            queue.releaseWorker();
            assertEquals(1L, first.get(5, TimeUnit.SECONDS));
        } finally {
            queue.releaseWorker();
            runtime.close();
            executor.shutdownNow();
        }
    }

}
