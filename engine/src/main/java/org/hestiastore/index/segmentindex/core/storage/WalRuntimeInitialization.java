package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Names the runtime collaborators required to initialize WAL coordination.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class WalRuntimeInitialization<K, V> {

    private final EffectiveIndexConfiguration<K, V> configuration;
    private final WalRuntime<K, V> walRuntime;
    private final WalCheckpointDurableState durableState;
    private final SegmentIndexStateView stateView;
    private final WalRuntimeFailureHandler failureHandler;
    private final AtomicLong lastAppliedWalLsn;

    /**
     * Creates WAL runtime initialization data.
     *
     * @param configuration effective index configuration
     * @param walRuntime WAL runtime, required when WAL is enabled
     * @param durableState durable state flushed before WAL checkpointing
     * @param stateView current index state view
     * @param failureHandler WAL runtime failure handler
     * @param lastAppliedWalLsn last durable WAL LSN tracker
     */
    public WalRuntimeInitialization(
            final EffectiveIndexConfiguration<K, V> configuration,
            final WalRuntime<K, V> walRuntime,
            final WalCheckpointDurableState durableState,
            final SegmentIndexStateView stateView,
            final WalRuntimeFailureHandler failureHandler,
            final AtomicLong lastAppliedWalLsn) {
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
        this.walRuntime = walRuntime;
        this.durableState = Vldtn.requireNonNull(durableState,
                "durableState");
        this.stateView = Vldtn.requireNonNull(stateView, "stateView");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        Vldtn.requireTrue(!this.configuration.wal().isEnabled()
                || this.walRuntime != null,
                "walRuntime must be initialized when WAL is enabled.");
    }

    EffectiveIndexConfiguration<K, V> configuration() {
        return configuration;
    }

    WalRuntime<K, V> walRuntime() {
        return Vldtn.requireNonNull(walRuntime, "walRuntime");
    }

    WalCheckpointDurableState durableState() {
        return durableState;
    }

    SegmentIndexStateView stateView() {
        return stateView;
    }

    WalRuntimeFailureHandler failureHandler() {
        return failureHandler;
    }

    AtomicLong lastAppliedWalLsn() {
        return lastAppliedWalLsn;
    }
}
