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
import org.hestiastore.index.segment.SegmentImplSynchronizationAdapter;
import org.hestiastore.index.segmentasync.SegmentAsync;
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
    private final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator;
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
        this.maintenanceCoordinator = new SegmentMaintenanceCoordinator<>(conf,
                keySegmentCache, segmentRegistry);
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
            compactSegment(seg);
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
            maintenanceCoordinator.handlePostWrite(segment, key, segmentId,
                    snapshot.version());
            return;
        }
    }

    private boolean writeIntoSegment(final Segment<K, V> segment, final K key,
            final V value, final SegmentId segmentId,
            final long mappingVersion) {
        if (segment instanceof SegmentImplSynchronizationAdapter<K, V> adapter) {
            return Boolean.TRUE.equals(adapter.executeWithWriteLock(() -> {
                if (segment.wasClosed()) {
                    return Boolean.FALSE;
                }
                if (!keySegmentCache.isMappingValid(key, segmentId,
                        mappingVersion)) {
                    return Boolean.FALSE;
                }
                segment.put(key, value);
                return Boolean.TRUE;
            }));
        }
        if (segment.wasClosed()) {
            return false;
        }
        if (!keySegmentCache.isMappingValid(key, segmentId, mappingVersion)) {
            return false;
        }
        segment.put(key, value);
        return true;
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
            flushSegment(segment);
        });
    }

    private void compactSegment(final Segment<K, V> segment) {
        if (segment instanceof SegmentAsync<K, V> async) {
            async.compactAsync().toCompletableFuture().join();
            return;
        }
        segment.compact();
    }

    private void flushSegment(final Segment<K, V> segment) {
        if (segment instanceof SegmentAsync<K, V> async) {
            async.flushAsync().toCompletableFuture().join();
            return;
        }
        segment.flush();
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
