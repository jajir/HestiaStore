package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentSynchronizationAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentSplitCoordinator<K, V> segmentSplitCoordinator;
    private final Stats stats = new Stats();
    private volatile IndexState<K, V> indexState;

    protected SegmentIndexImpl(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        setIndexState(new IndexStateNew<>(directoryFacade));
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keySegmentCache = new KeySegmentCache<>(directoryFacade,
                keyTypeDescriptor);
        this.segmentRegistry = new SegmentRegistrySynchronized<>(
                directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf);
        this.segmentSplitCoordinator = new SegmentSplitCoordinator<>(conf,
                keySegmentCache);
        getIndexState().onReady(this);
    }

    @Override
    public void put(final K key, final V value) {
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        stats.incPutCx();

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert thombstone value '%s' into index", value));
        }

        writeWithRetry(key, value);
    }

    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return CompletableFuture.runAsync(() -> {
            put(key, value);
        });
    }

    /**
     * return segment iterator. It doesn't count with mein cache.
     * 
     * @param segmentId required segment id
     * @return
     */
    EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final Segment<K, V> seg = segmentRegistry.getSegment(segmentId);
        return seg.openIterator();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(
            SegmentWindow segmentWindows) {
        if (segmentWindows == null) {
            segmentWindows = SegmentWindow.unbounded();
        }
        final EntryIterator<K, V> segmentIterator = new SegmentsIterator<>(
                keySegmentCache.getSegmentIds(segmentWindows), segmentRegistry);
        if (conf.isContextLoggingEnabled()) {
            return new EntryIteratorLoggingContext<>(segmentIterator, conf);
        } else {
            return segmentIterator;
        }
    }

    @Override
    public void compact() {
        getIndexState().tryPerformOperation();
        keySegmentCache.getSegmentIds().forEach(segmentId -> {
            final Segment<K, V> seg = segmentRegistry.getSegment(segmentId);
            seg.forceCompact();
        });
    }

    @Override
    public V get(final K key) {
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        return getFromSegment(key);
    }

    @Override
    public CompletionStage<V> getAsync(final K key) {
        return CompletableFuture.supplyAsync(() -> {
            return get(key);
        });
    }

    private V getFromSegment(final K key) {
        final SegmentId id = keySegmentCache.findSegmentId(key);
        if (id == null) {
            return null;
        }
        final Segment<K, V> segment = segmentRegistry.getSegment(id);
        return segment.get(key);
    }

    private void writeWithRetry(final K key, final V value) {
        while (true) {
            KeySegmentCache.Snapshot<K> snapshot = keySegmentCache.snapshot();
            SegmentId segmentId = snapshot.findSegmentId(key);
            if (segmentId == null) {
                if (!keySegmentCache.tryExtendMaxKey(key, snapshot)) {
                    continue;
                }
                snapshot = keySegmentCache.snapshot();
                segmentId = snapshot.findSegmentId(key);
                if (segmentId == null) {
                    continue;
                }
            }
            final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
            final boolean written = writeIntoSegment(segment, key, value,
                    segmentId, snapshot.version());
            if (!written) {
                continue;
            }
            handlePostWriteMaintenance(segment);
            return;
        }
    }

    private boolean writeIntoSegment(final Segment<K, V> segment, final K key,
            final V value, final SegmentId segmentId,
            final long mappingVersion) {
        if (segment instanceof SegmentSynchronizationAdapter<K, V> adapter) {
            return Boolean.TRUE.equals(adapter.executeWithWriteLock(() -> {
                if (!keySegmentCache.isMappingValid(key, segmentId,
                        mappingVersion)) {
                    return Boolean.FALSE;
                }
                segment.put(key, value);
                return Boolean.TRUE;
            }));
        }
        if (!keySegmentCache.isMappingValid(key, segmentId, mappingVersion)) {
            return false;
        }
        segment.put(key, value);
        return true;
    }

    private void handlePostWriteMaintenance(final Segment<K, V> segment) {
        final Integer maxWriteCacheKeys = conf
                .getMaxNumberOfKeysInSegmentWriteCache();
        if (maxWriteCacheKeys == null || maxWriteCacheKeys < 1) {
            return;
        }
        if (segment.getWriteCacheSize() < maxWriteCacheKeys.intValue()) {
            return;
        }

        final Integer maxSegmentCacheKeys = conf
                .getMaxNumberOfKeysInSegmentCache();
        if (maxSegmentCacheKeys != null && maxSegmentCacheKeys > 0) {
            final long totalKeys = segment.getTotalNumberOfKeysInCache();
            if (totalKeys > maxSegmentCacheKeys.longValue()) {
                final boolean split = segmentSplitCoordinator.optionallySplit(
                        segment, maxSegmentCacheKeys.longValue());
                if (!split) {
                    segment.flush();
                }
                return;
            }
        }
        segment.flush();
    }

    @Override
    public void delete(final K key) {
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incDeleteCx();
        writeWithRetry(key, valueTypeDescriptor.getTombstone());
    }

    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return CompletableFuture.runAsync(() -> {
            delete(key);
        });
    }

    @Override
    public void checkAndRepairConsistency() {
        getIndexState().tryPerformOperation();
        keySegmentCache.checkUniqueSegmentIds();
        final IndexConsistencyChecker<K, V> checker = new IndexConsistencyChecker<>(
                keySegmentCache, segmentRegistry, keyTypeDescriptor);
        checker.checkAndRepairConsistency();
    }

    @Override
    protected void doClose() {
        getIndexState().onClose(this);
        flushSegments();
        segmentRegistry.close();
        keySegmentCache.optionalyFlush();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format(
                    "Index is closing, where was %s gets, %s puts and %s deletes.",
                    F.fmt(stats.getGetCx()), F.fmt(stats.getPutCx()),
                    F.fmt(stats.getDeleteCx())));
        }
    }

    final void setIndexState(final IndexState<K, V> indexState) {
        this.indexState = Vldtn.requireNonNull(indexState, "indexState");
    }

    protected final IndexState<K, V> getIndexState() {
        return indexState;
    }

    @Override
    public void flush() {
        flushSegments();
        keySegmentCache.optionalyFlush();
    }

    private void flushSegments() {
        keySegmentCache.getSegmentIds().forEach(segmentId -> {
            final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
            segment.flush();
        });
    }

    protected void invalidateSegmentIterators() {
        keySegmentCache.getSegmentIds().forEach(segmentId -> {
            final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
            segment.invalidateIterators();
        });
    }

    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
