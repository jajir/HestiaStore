package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Public segment implementation that delegates single-threaded work to
 * {@link SegmentCore}.
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
public class SegmentImpl<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final SegmentCore<K, V> core;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentStateMachine stateMachine = new SegmentStateMachine();

    public SegmentImpl(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentResources,
            final SegmentDeltaCacheController<K, V> segmentDeltaCacheController,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentCompacter<K, V> segmentCompacter) {
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.core = new SegmentCore<>(segmentFiles, segmentConf,
                versionController, segmentPropertiesManager, segmentResources,
                segmentDeltaCacheController, segmentSearcher);
    }

    @Override
    public SegmentStats getStats() {
        return core.getStats();
    }

    @Override
    public long getNumberOfKeys() {
        return core.getNumberOfKeys();
    }

    @Override
    public K checkAndRepairConsistency() {
        final SegmentConsistencyChecker<K, V> consistencyChecker = new SegmentConsistencyChecker<>(
                this, core.getKeyComparator());
        return consistencyChecker.checkAndRepairConsistency();
    }

    @Override
    public void invalidateIterators() {
        core.invalidateIterators();
    }

    @Override
    public SegmentResult<EntryIterator<K, V>> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Override
    public SegmentResult<EntryIterator<K, V>> openIterator(
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            if (!stateMachine.tryEnterFreeze()) {
                return resultForState(stateMachine.getState());
            }
            core.invalidateIterators();
            final EntryIterator<K, V> iterator = core.openIterator(isolation);
            return SegmentResult.ok(
                    new ExclusiveAccessIterator<>(iterator, stateMachine));
        }
        final SegmentState state = stateMachine.getState();
        if (state != SegmentState.READY
                && state != SegmentState.MAINTENANCE_RUNNING) {
            return resultForState(state);
        }
        return SegmentResult.ok(core.openIterator(isolation));
    }

    @Override
    public SegmentResult<Void> compact() {
        if (!stateMachine.tryEnterFreeze()) {
            return resultForState(stateMachine.getState());
        }
        if (!stateMachine.enterMaintenanceRunning()) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        try {
            segmentCompacter.forceCompact(core);
        } catch (final RuntimeException e) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        if (!stateMachine.finishMaintenanceToFreeze()) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        if (!stateMachine.finishFreezeToReady()) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        return SegmentResult.ok();
    }

    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        core.executeFullWriteTx(writeFunction);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return core.openFullWriteTx();
    }

    @Override
    public SegmentResult<Void> put(final K key, final V value) {
        final SegmentState state = stateMachine.getState();
        if (state != SegmentState.READY
                && state != SegmentState.MAINTENANCE_RUNNING) {
            return resultForState(state);
        }
        if (!core.tryPutWithoutWaiting(key, value)) {
            return SegmentResult.busy();
        }
        return SegmentResult.ok();
    }

    boolean tryPutWithoutWaiting(final K key, final V value) {
        return core.tryPutWithoutWaiting(key, value);
    }

    void awaitWriteCapacity() {
        core.awaitWriteCapacity();
    }

    @Override
    public SegmentResult<Void> flush() {
        if (!stateMachine.tryEnterFreeze()) {
            return resultForState(stateMachine.getState());
        }
        if (!stateMachine.enterMaintenanceRunning()) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        try {
            core.flush();
        } catch (final RuntimeException e) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        if (!stateMachine.finishMaintenanceToFreeze()) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        if (!stateMachine.finishFreezeToReady()) {
            stateMachine.fail();
            return SegmentResult.error();
        }
        return SegmentResult.ok();
    }

    @Override
    public int getNumberOfKeysInWriteCache() {
        return core.getNumberOfKeysInWriteCache();
    }

    @Override
    public long getNumberOfKeysInCache() {
        return core.getNumberOfKeysInCache();
    }

    @Override
    public SegmentResult<V> get(final K key) {
        final SegmentState state = stateMachine.getState();
        if (state == SegmentState.READY
                || state == SegmentState.MAINTENANCE_RUNNING) {
            return SegmentResult.ok(core.get(key));
        }
        return resultForState(state);
    }

    @Override
    public SegmentId getId() {
        return core.getId();
    }

    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        return core.getSegmentIndexSearcher();
    }

    FileReaderSeekable getSeekableReader() {
        return core.getSeekableReader();
    }

    void resetSegmentIndexSearcher() {
        core.resetSegmentIndexSearcher();
    }

    void resetSeekableReader() {
        core.resetSeekableReader();
    }

    List<Entry<K, V>> freezeWriteCacheForFlush() {
        return core.freezeWriteCacheForFlush();
    }

    void flushFrozenWriteCacheToDeltaFile(final List<Entry<K, V>> entries) {
        core.flushFrozenWriteCacheToDeltaFile(entries);
    }

    void applyFrozenWriteCacheAfterFlush() {
        core.applyFrozenWriteCacheAfterFlush();
    }

    @Override
    protected void doClose() {
        stateMachine.forceClosed();
        core.close();
    }

    private static <T> SegmentResult<T> resultForState(
            final SegmentState state) {
        if (state == SegmentState.CLOSED) {
            return SegmentResult.closed();
        }
        if (state == SegmentState.ERROR) {
            return SegmentResult.error();
        }
        return SegmentResult.busy();
    }

    private static final class ExclusiveAccessIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        private final EntryIterator<K, V> delegate;
        private final SegmentStateMachine stateMachine;

        ExclusiveAccessIterator(final EntryIterator<K, V> delegate,
                final SegmentStateMachine stateMachine) {
            this.delegate = delegate;
            this.stateMachine = stateMachine;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            return delegate.next();
        }

        @Override
        protected void doClose() {
            try {
                delegate.close();
            } finally {
                stateMachine.finishFreezeToReady();
            }
        }
    }
}
