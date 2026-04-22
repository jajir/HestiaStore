package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Tracks the highest replayed or applied WAL LSN observed during recovery and
 * runtime operation.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalReplayProgressTracker<K, V> {

    private final WalRuntime<K, V> walRuntime;
    private final AtomicLong lastAppliedWalLsn;

    WalReplayProgressTracker(final WalRuntime<K, V> walRuntime,
            final AtomicLong lastAppliedWalLsn) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
    }

    void recover(final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        if (!walRuntime.isEnabled()) {
            return;
        }
        recordRecoveredLsn(walRuntime.recover(replayConsumer));
    }

    void recordAppliedLsn(final long walLsn) {
        if (walLsn <= 0L || !walRuntime.isEnabled()) {
            return;
        }
        while (true) {
            final long current = lastAppliedWalLsn.get();
            if (walLsn <= current) {
                return;
            }
            if (lastAppliedWalLsn.compareAndSet(current, walLsn)) {
                return;
            }
        }
    }

    private void recordRecoveredLsn(
            final WalRuntime.RecoveryResult recoveryResult) {
        if (recoveryResult.maxLsn() > 0L) {
            lastAppliedWalLsn.accumulateAndGet(recoveryResult.maxLsn(),
                    Math::max);
        }
    }
}
