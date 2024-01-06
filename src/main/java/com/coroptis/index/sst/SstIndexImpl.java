package com.coroptis.index.sst;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.segment.MergeIterator;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.sstfile.PairComparator;

public class SstIndexImpl<K, V> implements Index<K, V> {

    private final Logger logger = LoggerFactory.getLogger(SstIndexImpl.class);

    private final SsstIndexConf conf;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentManager<K, V> segmentManager;
    private IndexState<K, V> indexState;

    public SstIndexImpl(final Directory directory,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor, final SsstIndexConf conf) {
        Objects.requireNonNull(directory);
        indexState = new IndexStateNew<>(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.conf = Objects.requireNonNull(conf);
        this.cache = new UniqueCache<K, V>(
                this.keyTypeDescriptor.getComparator());
        this.keySegmentCache = new KeySegmentCache<>(directory,
                keyTypeDescriptor);
        this.segmentManager = new SegmentManager<>(directory, keyTypeDescriptor,
                valueTypeDescriptor, conf);
        indexState.onReady(this);
    }

    @Override
    public void put(final K key, final V value) {
        indexState.tryPerformOperation();
        Objects.requireNonNull(key, "Key cant be null");
        Objects.requireNonNull(value, "Value cant be null");

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert thombstone value '%s' into index", value));
        }

        // TODO add key value pair into WAL

        cache.put(Pair.of(key, value));

        if (cache.size() > conf.getMaxNumberOfKeysInCache()) {
            compact();
        }
    }

    public List<SegmentId> getSegmentIds() {
        return keySegmentCache.getSegmentsAsStream()
                .map(pair -> pair.getValue())
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * This is correct way to obtain data from segment.
     * 
     * @param segmentId required segment id
     * @return
     */
    public Stream<Pair<K, V>> getSegmentStream(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "SegmentId can't be null.");
        final Segment<K, V> seg = getSegment(segmentId);
        final PairIterator<K, V> limitedSegment = new LimitedPairIterator<>(
                cache.getSortedIterator(), keyTypeDescriptor.getComparator(),
                seg.getMinKey(), seg.getMaxKey());
        final PairIterator<K, V> out = new MergeIterator<K, V>(
                seg.openIterator(), limitedSegment, keyTypeDescriptor,
                valueTypeDescriptor);
        return StreamSupport.stream(
                new PairIteratorToSpliterator<K, V>(out, keyTypeDescriptor),
                false);
    }

    /**
     * return segment iterator. It doesn't count with mein cache.
     * 
     * @param segmentId required segment id
     * @return
     */
    PairIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "SegmentId can't be null.");
        final Segment<K, V> seg = getSegment(segmentId);
        return seg.openIterator();
    }

    public PairIterator<K, V> openIterator() {
        final PairIterator<K, V> segments = new SegmentsIterator<>(this);
        return new MergeIterator<K, V>(segments, cache.getSortedIterator(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    public Stream<Pair<K, V>> getStream() {
        indexState.tryPerformOperation();
        return StreamSupport.stream(new PairIteratorToSpliterator<K, V>(
                openIterator(), keyTypeDescriptor), false);
    }

    private void compact() {
        logger.debug(
                "Cache compacting of '{}' key value pairs in cache started.",
                cache.size());
        final CompactSupport<K, V> support = new CompactSupport<>(
                segmentManager, keySegmentCache,conf.getMaxNumberOfKeysInSegmentCache());
        cache.getStream()
                .sorted(new PairComparator<>(keyTypeDescriptor.getComparator()))
                .forEach(support::compact);
        support.compactRest();
        List<SegmentId> segmentIds = support.getEligibleSegmentIds();
        segmentIds.stream().map(this::getSegment)
                .forEach(this::optionallySplit);
        cache.clear();
        keySegmentCache.flush();
        logger.debug(
                "Cache compacting is done. Cache contains '{}' key value pairs.",
                cache.size());
    }

    Segment<K, V> getSegment(final SegmentId segmentId) {
        return segmentManager.getSegment(segmentId);
    }

    @Override
    public void forceCompact() {
        indexState.tryPerformOperation();
        compact();
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param segment required simple data file
     * @return
     */
    private boolean optionallySplit(final Segment<K, V> segment) {
        Objects.requireNonNull(segment, "Segment is required");
        if (segment.getStats().getNumberOfKeys() > conf
                .getMaxNumberOfKeysInSegment()) {
            final SegmentId segmentId = segment.getId();
            logger.debug("Splitting of '{}' started.", segmentId);
            final SegmentId newSegmentId = keySegmentCache.findNewSegmentId();
            final Segment<K, V> splitted = segment.split(newSegmentId);
            keySegmentCache.insertSegment(splitted.getMaxKey(), newSegmentId);
            logger.debug("Splitting of segment '{}' to '{}' is done.",
                    segmentId, newSegmentId);
            return true;
        }
        return false;
    }

    @Override
    public V get(final K key) {
        indexState.tryPerformOperation();
        Objects.requireNonNull(key, "Key cant be null");

        V out = cache.get(key);
        if (out == null) {
            final SegmentId id = keySegmentCache.findSegmentId(key);
            if (id == null) {
                return null;
            }
            final Segment<K, V> seg = getSegment(id);
            return seg.get(key);
        }

        return out;
    }

    @Override
    public void delete(final K key) {
        indexState.tryPerformOperation();
        Objects.requireNonNull(key, "Key cant be null");

        cache.put(Pair.of(key, valueTypeDescriptor.getTombstone()));
    }

    @Override
    public void close() {
        compact();
        indexState.onClose(this);
    }

    public void setIndexState(final IndexState<K, V> indexState) {
        this.indexState = Objects.requireNonNull(indexState);
    }

    SegmentManager<K, V> getSegmentManager() {
        return segmentManager;
    }

}
