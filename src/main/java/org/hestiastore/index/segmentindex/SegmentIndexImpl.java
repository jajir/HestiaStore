package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.cache.UniqueCacheBuilder;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentSplitCoordinator<K, V> segmentSplitCoordinator;
    private final Stats stats = new Stats();
    private volatile IndexState<K, V> indexState;
    private final ReentrantLock flushLock = new ReentrantLock();

    protected SegmentIndexImpl(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        if (directory == null) {
            throw new IllegalArgumentException("Directory was no specified.");
        }
        Vldtn.requireNonNull(directory, "directory");
        setIndexState(new IndexStateNew<>(directory));
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        final UniqueCacheBuilder<K, V> cacheBuilder = UniqueCache
                .<K, V>builder()
                .withKeyComparator(this.keyTypeDescriptor.getComparator())
                .withThreadSafe(true);
        final Integer cacheCapacity = conf.getMaxNumberOfKeysInCache();
        if (cacheCapacity != null && cacheCapacity > 0) {
            cacheBuilder.withInitialCapacity(cacheCapacity);
        }
        this.cache = cacheBuilder.buildEmpty();
        this.keySegmentCache = new KeySegmentCache<>(directory,
                keyTypeDescriptor);
        final SegmentDataCache<K, V> segmentDataCache = new SegmentDataCache<>(
                conf);
        this.segmentRegistry = new SegmentRegistrySynchronized<>(directory,
                keyTypeDescriptor, valueTypeDescriptor, conf, segmentDataCache);
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

        cache.put(Entry.of(key, value));

        flushCacheIfNeeded();
    }

    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this) {
                put(key, value);
            }
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
        final EntryIterator<K, V> iterratorFreshedFromCache = new EntryIteratorRefreshedFromCache<>(
                segmentIterator, cache, valueTypeDescriptor);
        if (conf.isContextLoggingEnabled()) {
            return new EntryIteratorLoggingContext<>(iterratorFreshedFromCache,
                    conf);
        } else {
            return iterratorFreshedFromCache;
        }
    }

    void flushCacheIfNeeded() {
        if (cache.size() > conf.getMaxNumberOfKeysInCache()) {
            flushCache();
        }
    }

    protected void flushCache() {
        flushLock.lock();
        try {
            final List<Entry<K, V>> snapshot = cache.getAsSortedList();
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Cache compacting of '{}' key value entries in cache started.",
                        F.fmt(snapshot.size()));
            }
            final CompactSupport<K, V> support = new CompactSupport<>(
                    segmentRegistry, keySegmentCache,
                    keyTypeDescriptor.getComparator());
            snapshot.forEach(support::compact);
            support.flush();
            final List<SegmentId> segmentIds = support.getEligibleSegmentIds();
            for (final SegmentId segmentId : segmentIds) {
                final Segment<K, V> segment = segmentRegistry
                        .getSegment(segmentId);
                if (segmentSplitCoordinator.shouldBeSplit(segment)) {
                    segmentSplitCoordinator.optionallySplit(segment);
                }
            }
            cache.clear();
            keySegmentCache.optionalyFlush();
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Cache compacting is done. Cache contains '{}' key value entries.",
                        F.fmt(cache.size()));
            }
        } finally {
            flushLock.unlock();
        }
    }

    @Override
    public void compact() {
        getIndexState().tryPerformOperation();
        flushCache();
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

        final V cachedWrite = cache.get(key);
        if (cachedWrite != null) {
            if (valueTypeDescriptor.isTombstone(cachedWrite)) {
                return null;
            }
            return cachedWrite;
        }

        return getFromSegment(key);
    }

    @Override
    public CompletionStage<V> getAsync(final K key) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this) {
                return get(key);
            }
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

    @Override
    public void delete(final K key) {
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incDeleteCx();

        cache.put(Entry.of(key, valueTypeDescriptor.getTombstone()));
        flushCacheIfNeeded();
    }

    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this) {
                delete(key);
            }
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
        flushCache();
        getIndexState().onClose(this);
        segmentRegistry.close();
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
        flushCache();
    }

    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
