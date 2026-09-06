package org.hestiastore.index.senku.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.IndexException;

/**
 * Process-wide CPU and byte budgets shared by flush sorting and page
 * preparation. Preparation tasks do not block or acquire permits from inside
 * this pool.
 */
final class SenkuFlushExecutor {
    static final int PAGE_BYTES_BUDGET = 64 * 1024 * 1024;
    private static final int PARALLELISM = Math.max(1,
            Math.min(4, Runtime.getRuntime().availableProcessors()));
    private static final AtomicInteger NEXT_WORKER_ID = new AtomicInteger(1);
    private static final ForkJoinPool POOL = new ForkJoinPool(PARALLELISM,
            SenkuFlushExecutor::newWorker, null, false);
    private static final Semaphore PAGE_BYTES = new Semaphore(
            PAGE_BYTES_BUDGET);

    private SenkuFlushExecutor() {
    }

    /** @return target CPU parallelism shared by all flushes */
    static int parallelism() {
        return PARALLELISM;
    }

    /** Submits a CPU-only task; the caller retains lifecycle ownership. */
    static <T> ForkJoinTask<T> submit(final ForkJoinTask<T> task) {
        return POOL.submit(task);
    }

    /**
     * Reserves page working bytes without waiting while holding other pages.
     */
    static boolean tryReserve(final int bytes) {
        return PAGE_BYTES.tryAcquire(bytes);
    }

    /** Waits only when the caller has no outstanding preparation tasks. */
    static void reserve(final int bytes) {
        try {
            PAGE_BYTES.acquire(bytes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IndexException("Interrupted awaiting flush page memory.",
                    e);
        }
    }

    /**
     * Releases a reservation after its task has finished and bytes are
     * consumed.
     */
    static void release(final int bytes) {
        PAGE_BYTES.release(bytes);
    }

    private static ForkJoinWorkerThread newWorker(final ForkJoinPool pool) {
        final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory
                .newThread(pool);
        worker.setName(
                "senku-flush-prepare-" + NEXT_WORKER_ID.getAndIncrement());
        worker.setDaemon(true);
        return worker;
    }
}
