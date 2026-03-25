package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.slf4j.Logger;

/**
 * Assembles the runtime collaborators used by {@link SegmentIndexImpl} and
 * completes the startup sequence.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexAssembly<K, V> {

    /**
     * Callback bundle used by runtime assembly and close/open coordination.
     */
    @SuppressWarnings("java:S107")
    static final class Callbacks {

        private final Supplier<SegmentIndexState> stateSupplier;
        private final Runnable awaitSplitsIdle;
        private final Consumer<RuntimeException> failureHandler;
        private final Runnable onBackgroundSplitApplied;
        private final Runnable beginCloseTransition;
        private final Runnable awaitOperations;
        private final Runnable markClosed;
        private final LongSupplier getReadCount;
        private final LongSupplier getWriteCount;
        private final LongSupplier getDeleteCount;
        private final Runnable finishCloseTransition;

        Callbacks(final Supplier<SegmentIndexState> stateSupplier,
                final Runnable awaitSplitsIdle,
                final Consumer<RuntimeException> failureHandler,
                final Runnable onBackgroundSplitApplied,
                final Runnable beginCloseTransition,
                final Runnable awaitOperations,
                final Runnable markClosed,
                final LongSupplier getReadCount,
                final LongSupplier getWriteCount,
                final LongSupplier getDeleteCount,
                final Runnable finishCloseTransition) {
            this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                    "stateSupplier");
            this.awaitSplitsIdle = Vldtn.requireNonNull(awaitSplitsIdle,
                    "awaitSplitsIdle");
            this.failureHandler = Vldtn.requireNonNull(failureHandler,
                    "failureHandler");
            this.onBackgroundSplitApplied = Vldtn.requireNonNull(
                    onBackgroundSplitApplied, "onBackgroundSplitApplied");
            this.beginCloseTransition = Vldtn.requireNonNull(
                    beginCloseTransition, "beginCloseTransition");
            this.awaitOperations = Vldtn.requireNonNull(awaitOperations,
                    "awaitOperations");
            this.markClosed = Vldtn.requireNonNull(markClosed, "markClosed");
            this.getReadCount = Vldtn.requireNonNull(getReadCount,
                    "getReadCount");
            this.getWriteCount = Vldtn.requireNonNull(getWriteCount,
                    "getWriteCount");
            this.getDeleteCount = Vldtn.requireNonNull(getDeleteCount,
                    "getDeleteCount");
            this.finishCloseTransition = Vldtn.requireNonNull(
                    finishCloseTransition, "finishCloseTransition");
        }

        Supplier<SegmentIndexState> stateSupplier() {
            return stateSupplier;
        }

        Runnable awaitSplitsIdle() {
            return awaitSplitsIdle;
        }

        Consumer<RuntimeException> failureHandler() {
            return failureHandler;
        }

        Runnable onBackgroundSplitApplied() {
            return onBackgroundSplitApplied;
        }

        Runnable beginCloseTransition() {
            return beginCloseTransition;
        }

        Runnable awaitOperations() {
            return awaitOperations;
        }

        Runnable markClosed() {
            return markClosed;
        }

        LongSupplier getReadCount() {
            return getReadCount;
        }

        LongSupplier getWriteCount() {
            return getWriteCount;
        }

        LongSupplier getDeleteCount() {
            return getDeleteCount;
        }

        Runnable finishCloseTransition() {
            return finishCloseTransition;
        }
    }

    private final SegmentIndexRuntime<K, V> runtime;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;
    private final IndexCloseCoordinator closeCoordinator;

    @SuppressWarnings("java:S107")
    private SegmentIndexAssembly(final SegmentIndexRuntime<K, V> runtime,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator,
            final IndexCloseCoordinator closeCoordinator) {
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
    }

    static <K, V> SegmentIndexAssembly<K, V> open(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Callbacks callbacks) {
        final Logger nonNullLogger = Vldtn.requireNonNull(logger, "logger");
        final Directory nonNullDirectory = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        final TypeDescriptor<K> nonNullKeyTypeDescriptor = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> nonNullValueTypeDescriptor = Vldtn
                .requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        final IndexConfiguration<K, V> nonNullConf = Vldtn.requireNonNull(conf,
                "conf");
        final IndexRuntimeConfiguration<K, V> nonNullRuntimeConfiguration = Vldtn
                .requireNonNull(runtimeConfiguration, "runtimeConfiguration");
        final IndexExecutorRegistry nonNullExecutorRegistry = Vldtn
                .requireNonNull(executorRegistry, "executorRegistry");
        final Stats nonNullStats = Vldtn.requireNonNull(stats, "stats");
        final AtomicLong nonNullCompactRequestHighWaterMark = Vldtn
                .requireNonNull(compactRequestHighWaterMark,
                        "compactRequestHighWaterMark");
        final AtomicLong nonNullFlushRequestHighWaterMark = Vldtn
                .requireNonNull(flushRequestHighWaterMark,
                        "flushRequestHighWaterMark");
        final AtomicLong nonNullLastAppliedWalLsn = Vldtn
                .requireNonNull(lastAppliedWalLsn, "lastAppliedWalLsn");
        final Callbacks nonNullCallbacks = Vldtn.requireNonNull(callbacks,
                "callbacks");

        final SegmentIndexRuntime<K, V> runtime = SegmentIndexRuntime.open(
                nonNullLogger, nonNullDirectory, nonNullKeyTypeDescriptor,
                nonNullValueTypeDescriptor, nonNullConf,
                nonNullRuntimeConfiguration, nonNullExecutorRegistry, nonNullStats,
                nonNullCompactRequestHighWaterMark,
                nonNullFlushRequestHighWaterMark, nonNullLastAppliedWalLsn,
                nonNullCallbacks.stateSupplier(),
                nonNullCallbacks.awaitSplitsIdle(),
                nonNullCallbacks.failureHandler(),
                nonNullCallbacks.onBackgroundSplitApplied());
        final IndexConsistencyCoordinator<K, V> consistencyCoordinator = new IndexConsistencyCoordinator<>(
                runtime.keyToSegmentMap(), runtime.segmentRegistry(),
                nonNullKeyTypeDescriptor,
                runtime.recoveryCleanupCoordinator(),
                runtime.backgroundSplitPolicyLoop());
        final IndexCloseCoordinator closeCoordinator = runtime
                .newCloseCoordinator(nonNullLogger, nonNullConf.getIndexName(),
                        nonNullCallbacks.beginCloseTransition(),
                        nonNullCallbacks.awaitOperations(),
                        nonNullCallbacks.markClosed(),
                        nonNullCallbacks.getReadCount(),
                        nonNullCallbacks.getWriteCount(),
                        nonNullCallbacks.getDeleteCount(),
                        nonNullCallbacks.finishCloseTransition());
        return new SegmentIndexAssembly<>(runtime, consistencyCoordinator,
                closeCoordinator);
    }

    void completeOpen(final Logger logger, final String indexName,
            final boolean staleLockRecovered, final Runnable markReady,
            final Runnable startupConsistencyCheck) {
        final IndexOpenCoordinator openCoordinator = new IndexOpenCoordinator(
                Vldtn.requireNonNull(logger, "logger"),
                Vldtn.requireNonNull(indexName, "indexName"));
        final Runnable nonNullMarkReady = Vldtn.requireNonNull(markReady,
                "markReady");
        final Runnable nonNullStartupConsistencyCheck = Vldtn.requireNonNull(
                startupConsistencyCheck, "startupConsistencyCheck");
        openCoordinator.completeOpen(staleLockRecovered,
                () -> runtime.recover(
                        runtime.operationCoordinator()::replayWalRecord),
                runtime.recoveryCleanupCoordinator()::cleanupOrphanedSegmentDirectories,
                nonNullMarkReady,
                () -> consistencyCoordinator.runStartupConsistencyCheck(
                        nonNullStartupConsistencyCheck),
                runtime.backgroundSplitPolicyLoop()::scheduleScan);
    }

    SegmentIndexRuntime<K, V> runtime() {
        return runtime;
    }

    IndexConsistencyCoordinator<K, V> consistencyCoordinator() {
        return consistencyCoordinator;
    }

    IndexCloseCoordinator closeCoordinator() {
        return closeCoordinator;
    }
}
