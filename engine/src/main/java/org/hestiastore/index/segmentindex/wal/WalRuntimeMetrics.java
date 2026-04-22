package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

final class WalRuntimeMetrics {

    private final LongAdder appendCount = new LongAdder();
    private final LongAdder appendBytes = new LongAdder();
    private final LongAdder syncCount = new LongAdder();
    private final LongAdder syncTotalNanos = new LongAdder();
    private final LongAdder syncBatchBytesTotal = new LongAdder();
    private final LongAdder syncFailureCount = new LongAdder();
    private final LongAdder corruptionCount = new LongAdder();
    private final LongAdder truncationCount = new LongAdder();
    private final AtomicLong syncMaxNanos = new AtomicLong(0L);
    private final AtomicLong syncBatchBytesMax = new AtomicLong(0L);

    void recordAppend(final int bytes) {
        appendCount.increment();
        appendBytes.add(bytes);
    }

    void recordSyncSuccess(final long elapsedNanos, final long batchBytes) {
        syncCount.increment();
        syncTotalNanos.add(Math.max(0L, elapsedNanos));
        syncBatchBytesTotal.add(Math.max(0L, batchBytes));
        updateMax(syncMaxNanos, Math.max(0L, elapsedNanos));
        updateMax(syncBatchBytesMax, Math.max(0L, batchBytes));
    }

    void recordSyncFailure() {
        syncFailureCount.increment();
    }

    long syncFailureCount() {
        return syncFailureCount.sum();
    }

    void recordCorruption() {
        corruptionCount.increment();
    }

    void recordTruncation() {
        truncationCount.increment();
    }

    WalStats snapshot(final long retainedBytes, final int segmentCount,
            final long durableLsn, final long checkpointLsn,
            final long pendingSyncBytes) {
        return new WalStats(appendCount.sum(), appendBytes.sum(),
                syncCount.sum(), syncFailureCount.sum(), corruptionCount.sum(),
                truncationCount.sum(), retainedBytes, segmentCount, durableLsn,
                checkpointLsn, pendingSyncBytes, syncTotalNanos.sum(),
                syncMaxNanos.get(), syncBatchBytesTotal.sum(),
                syncBatchBytesMax.get());
    }

    static WalStats emptySnapshot() {
        return new WalStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0L, 0L, 0L, 0L, 0L,
                0L, 0L);
    }

    static void updateMax(final AtomicLong target, final long candidate) {
        while (true) {
            final long current = target.get();
            if (candidate <= current) {
                return;
            }
            if (target.compareAndSet(current, candidate)) {
                return;
            }
        }
    }
}
