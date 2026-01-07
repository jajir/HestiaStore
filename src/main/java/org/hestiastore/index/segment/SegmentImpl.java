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
    public EntryIterator<K, V> openIterator() {
        return core.openIterator();
    }

    @Override
    public EntryIterator<K, V> openIterator(
            final SegmentIteratorIsolation isolation) {
        return core.openIterator(isolation);
    }

    @Override
    public void compact() {
        segmentCompacter.forceCompact(this);
    }

    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        core.executeFullWriteTx(writeFunction);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return core.openFullWriteTx();
    }

    @Override
    public void put(final K key, final V value) {
        core.put(key, value);
    }

    boolean tryPutWithoutWaiting(final K key, final V value) {
        return core.tryPutWithoutWaiting(key, value);
    }

    void awaitWriteCapacity() {
        core.awaitWriteCapacity();
    }

    @Override
    public void flush() {
        core.flush();
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
    public V get(final K key) {
        return core.get(key);
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
        core.close();
    }
}
