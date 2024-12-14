package com.coroptis.index.scarceindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptor;

/**
 * Provide information about keys and particular index files. Each key
 * represents part of index equal or smaller to given key. Last key represents
 * higher key in index. When new value in index is entered it should be called
 * {@link #insertKeyToSegment(Object)}. This method update higher key value when
 * it's necessary.
 * 
 * @author honza
 *
 * @param <K>
 */
public class ScarceIndexCache<K> {

    private final Logger logger = LoggerFactory
            .getLogger(ScarceIndexCache.class);

    private final TreeMap<K, Integer> list;
    private final Comparator<K> keyComparator;

    ScarceIndexCache(final TypeDescriptor<K> keyTypeDescriptor) {
        Objects.requireNonNull(keyTypeDescriptor.getComparator());
        this.keyComparator = Objects
                .requireNonNull(keyTypeDescriptor.getComparator());
        this.list = new TreeMap<>(keyComparator);
    }

    void put(final Pair<K, Integer> pair) {
        Objects.requireNonNull(pair, "Pair is null.");
        list.put(pair.getKey(), pair.getValue());
    }

    /**
     * Verify that no different keys have same value (position in file) and
     * verify that value position in file just grow.
     */
    public void sanityCheck() {
        final AtomicBoolean fail = new AtomicBoolean(false);
        final List<Pair<K, Integer>> tmp = new ArrayList<>();
        list.entrySet().stream().forEach(entry -> {
            if (!tmp.isEmpty()) {
                final Pair<K, Integer> previous = tmp.get(0);
                if (keyComparator.compare(previous.getKey(),
                        entry.getKey()) >= 0) {
                    fail.set(true);
                    logger.error(String.format(
                            "Scarce index is not correctle ordered key '%s' is before  "
                                    + "key '%s' but first key is higher or equals then second one.",
                            previous.getKey(), entry.getKey()));
                }
                if (previous.getValue() >= entry.getValue()) {
                    fail.set(true);
                    logger.error(String.format(
                            "key '%s' and key '%s' should have correct order of values '%s' and '%s'.",
                            previous.getKey(), entry.getKey(),
                            previous.getValue(), entry.getValue()));
                }
            }
            tmp.add(0, Pair.of(entry.getKey(), entry.getValue()));
        });
        if (fail.get()) {
            throw new IllegalStateException(
                    "Unable to load scarce index, sanity check failed.");
        }
    }

    public Integer findSegmentId(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Pair<K, Integer> pair = localFindSegmentForKey(key);
        return pair == null ? null : pair.getValue();
    }

    private Pair<K, Integer> localFindSegmentForKey(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Map.Entry<K, Integer> ceilingEntry = list.floorEntry(key);
        if (ceilingEntry == null) {
            return null;
        } else {
            final K higherKey = list.lastKey();
            if (keyComparator.compare(key, higherKey) > 0) {
                // given key is higher that higher key in scarce and main index
                return null;
            } else {
                return Pair.of(ceilingEntry.getKey(), ceilingEntry.getValue());
            }
        }
    }

    public int getKeyCount() {
        return list.size();
    }

    public K getMinKey() {
        if (list.isEmpty()) {
            return null;
        }
        return list.firstKey();
    }

    public K getMaxKey() {
        if (list.isEmpty()) {
            return null;
        }
        return list.lastKey();
    }

    public void clear() {
        list.clear();
    }

    public Stream<Pair<K, Integer>> getSegmentsAsStream() {
        return list.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()));
    }

}
