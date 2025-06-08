package org.hestiastore.index.sst;

import java.util.List;

import org.hestiastore.index.F;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.log.Log;
import org.hestiastore.index.log.LoggedKey;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentSplitter;
import org.hestiastore.index.segment.SegmentSplitterResult;
import org.hestiastore.index.sorteddatafile.PairComparator;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFileStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SstIndexImpl<K, V> implements IndexInternal<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentManager<K, V> segmentManager;
    private final Log<K, V> log;
    private final Stats stats = new Stats();
    protected IndexState<K, V> indexState;

    public SstIndexImpl(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf, final Log<K, V> log) {
        if (directory == null) {
            throw new IllegalArgumentException("Directory was no specified.");
        }
        Vldtn.requireNonNull(directory, "directory");
        indexState = new IndexStateNew<>(directory);
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.log = Vldtn.requireNonNull(log, "log");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.cache = new UniqueCache<K, V>(
                this.keyTypeDescriptor.getComparator());
        this.keySegmentCache = new KeySegmentCache<>(directory,
                keyTypeDescriptor);
        final SegmentDataCache<K, V> segmentDataCache = new SegmentDataCache<>(
                conf);
        this.segmentManager = new SegmentManager<>(directory, keyTypeDescriptor,
                valueTypeDescriptor, conf, segmentDataCache);
        indexState.onReady(this);
    }

    @Override
    public void put(final K key, final V value) {
        indexState.tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        stats.incPutCx();

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert thombstone value '%s' into index", value));
        }

        // add key value pair into WAL
        log.post(key, value);

        cache.put(Pair.of(key, value));

        if (cache.size() > conf.getMaxNumberOfKeysInCache()) {
            flushCache();
        }
    }

    /**
     * return segment iterator. It doesn't count with mein cache.
     * 
     * @param segmentId required segment id
     * @return
     */
    PairIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final Segment<K, V> seg = segmentManager.getSegment(segmentId);
        return seg.openIterator();
    }

    @Override
    public PairIterator<K, V> openSegmentIterator(
            SegmentWindow segmentWindows) {
        if (segmentWindows == null) {
            segmentWindows = SegmentWindow.unbounded();
        }
        final PairIterator<K, V> segmentIterator = new SegmentsIterator<>(
                keySegmentCache.getSegmentIds(segmentWindows), segmentManager);
        final PairIterator<K, V> iterratorFreshedFromCache = new PairIteratorRefreshedFromCache<>(
                segmentIterator, cache, valueTypeDescriptor);
        final PairIteratorLoggingContext<K, V> pairIteratorLoggingContext = new PairIteratorLoggingContext<>(
                iterratorFreshedFromCache, conf);
        return pairIteratorLoggingContext;
    }

    private void flushCache() {
        logger.debug(
                "Cache compacting of '{}' key value pairs in cache started.",
                F.fmt(cache.size()));
        final CompactSupport<K, V> support = new CompactSupport<>(
                segmentManager, keySegmentCache);
        cache.getStream()
                .sorted(new PairComparator<>(keyTypeDescriptor.getComparator()))
                .forEach(support::compact);
        support.compactRest();
        final List<SegmentId> segmentIds = support.getEligibleSegmentIds();
        segmentIds.stream().map(segmentManager::getSegment)
                .forEach(this::optionallySplit);
        cache.clear();
        keySegmentCache.flush();
        log.rotate();
        logger.debug(
                "Cache compacting is done. Cache contains '{}' key value pairs.",
                F.fmt(cache.size()));
    }

    @Override
    public void compact() {
        indexState.tryPerformOperation();
        flushCache();
        keySegmentCache.getSegmentIds().forEach(segmentId -> {
            final Segment<K, V> seg = segmentManager.getSegment(segmentId);
            seg.forceCompact();
        });
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param segment required simple data file
     * @return
     */
    private boolean optionallySplit(final Segment<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        if (shouldBeSplit(segment)) {
            final SegmentSplitter<K, V> segmentSplitter = segment
                    .getSegmentSplitter();
            if (segmentSplitter.shouldBeCompactedBeforeSplitting(
                    conf.getMaxNumberOfKeysInSegment())) {
                segment.forceCompact();
                if (shouldBeSplit(segment)) {
                    return split(segment, segmentSplitter);
                }
            } else {
                return split(segment, segmentSplitter);
            }
        }
        return false;
    }

    private boolean shouldBeSplit(final Segment<K, V> segment) {
        return segment.getNumberOfKeys() > conf.getMaxNumberOfKeysInSegment();
    }

    private boolean split(final Segment<K, V> segment,
            final SegmentSplitter<K, V> segmentSplitter) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Splitting of '{}' started.", segmentId);
        final SegmentId newSegmentId = keySegmentCache.findNewSegmentId();
        final SegmentSplitterResult<K, V> result = segmentSplitter
                .split(newSegmentId);
        if (result.isSplited()) {
            keySegmentCache.insertSegment(result.getMaxKey(), newSegmentId);
            logger.debug("Splitting of segment '{}' to '{}' is done.",
                    segmentId, newSegmentId);
        } else {
            logger.debug(
                    "Splitting of segment '{}' is done, "
                            + "but at the end it was compacting.",
                    segmentId, newSegmentId);
        }
        return true;
    }

    @Override
    public V get(final K key) {
        indexState.tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final V out = cache.get(key);
        if (out == null) {
            final SegmentId id = keySegmentCache.findSegmentId(key);
            if (id == null) {
                return null;
            }
            final Segment<K, V> segment = segmentManager.getSegment(id);
            return segment.get(key);
        } else {
            if (valueTypeDescriptor.isTombstone(out)) {
                return null;
            } else {
                return out;
            }
        }
    }

    @Override
    public void delete(final K key) {
        indexState.tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incDeleteCx();

        log.delete(key, valueTypeDescriptor.getTombstone());

        cache.put(Pair.of(key, valueTypeDescriptor.getTombstone()));
    }

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> getLogStreamer() {
        return log.openStreamer();
    }

    @Override
    public void checkAndRepairConsistency() {
        indexState.tryPerformOperation();
        keySegmentCache.checkUniqueSegmentIds();
        final IndexConsistencyChecker<K, V> checker = new IndexConsistencyChecker<>(
                keySegmentCache, segmentManager, keyTypeDescriptor);
        checker.checkAndRepairConsistency();
    }

    @Override
    public void close() {
        flushCache();
        log.close();
        indexState.onClose(this);
        segmentManager.close();
        logger.debug(String.format(
                "Index is closing, where was %s gets, %s puts and %s deletes.",
                F.fmt(stats.getGetCx()), F.fmt(stats.getPutCx()),
                F.fmt(stats.getDeleteCx())));
    }

    public void setIndexState(final IndexState<K, V> indexState) {
        this.indexState = Vldtn.requireNonNull(indexState, "indexState");
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
