package com.hestiastore.index.sst;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hestiastore.index.CloseableResource;
import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.segment.SegmentId;
import com.hestiastore.index.sorteddatafile.SortedDataFile;
import com.hestiastore.index.sorteddatafile.SortedDataFileWriter;

/**
 * Provide information about keys and particular segment files. Each key
 * represents one segment. All keys segment are equal or smaller to given key.
 * Last key represents higher key in index. When new value in index is entered
 * it should be called {@link #insertKeyToSegment(Object)}. This method update
 * higher key value when it's necessary.
 *
 * Note that this is similar to scarce index, but still different.
 * 
 * @author honza
 *
 * @param <K>
 */
public class KeySegmentCache<K> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static TypeDescriptorSegmentId tdSegId = new TypeDescriptorSegmentId();

    private final static String FILE_NAME = "index.map";

    /**
     * When new index is created than this is id of first segment.
     */
    public final static SegmentId FIRST_SEGMENT_ID = SegmentId.of(0);

    private TreeMap<K, SegmentId> list;
    private final SortedDataFile<K, SegmentId> sdf;
    private final Comparator<K> keyComparator;
    private boolean isDirty = false;

    KeySegmentCache(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor) {
        Objects.requireNonNull(directory, "Directory object is null.");
        Objects.requireNonNull(keyTypeDescriptor,
                "Key type comparator is null.");
        this.keyComparator = Objects
                .requireNonNull(keyTypeDescriptor.getComparator());
        this.sdf = SortedDataFile.<K, SegmentId>builder() //
                .withDirectory(directory) //
                .withFileName(FILE_NAME)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(tdSegId) //
                .build();

        this.list = new TreeMap<>(keyComparator);
        try (PairIterator<K, SegmentId> reader = sdf.openIterator()) {
            while (reader.hasNext()) {
                final Pair<K, SegmentId> pair = reader.next();
                list.put(pair.getKey(), pair.getValue());
            }
        }
        checkUniqueSegmentIds();
    }

    /**
     * Verify, that all segment ids are unique.
     */
    public void checkUniqueSegmentIds() {
        final HashMap<SegmentId, K> tmp = new HashMap<SegmentId, K>();
        final AtomicBoolean fail = new AtomicBoolean(false);
        list.forEach((key, segmentId) -> {
            final K oldKey = tmp.get(segmentId);
            if (oldKey == null) {
                tmp.put(segmentId, key);
            } else {
                logger.error(String.format(
                        "Segment id '%s' is used for segment with "
                                + "key '%s' and segment with key '%s'.",
                        segmentId, key, oldKey));
                fail.set(true);
            }
        });
        if (fail.get()) {
            throw new IllegalStateException(
                    "Unable to load scarce index, sanity check failed.");
        }
    }

    public SegmentId findSegmentId(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Pair<K, SegmentId> pair = localFindSegmentForKey(key);
        return pair == null ? null : pair.getValue();
    }

    public SegmentId findNewSegmentId() {
        return SegmentId.of((int) (getSegmentsAsStream().count()));
    }

    public SegmentId insertKeyToSegment(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Pair<K, SegmentId> pair = localFindSegmentForKey(key);
        if (pair == null) {
            /*
             * Key is bigger that all key so it will at last segment. But key at
             * last segment is smaller than adding one. Because of that key have
             * to be upgraded.
             */
            isDirty = true;
            return updateMaxKey(key);
        } else {
            return pair.getValue();
        }
    }

    private Pair<K, SegmentId> localFindSegmentForKey(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Map.Entry<K, SegmentId> ceilingEntry = list.ceilingEntry(key);
        if (ceilingEntry == null) {
            return null;
        } else {
            return Pair.of(ceilingEntry.getKey(), ceilingEntry.getValue());
        }
    }

    private SegmentId updateMaxKey(final K key) {
        if (list.size() == 0) {
            list.put(key, FIRST_SEGMENT_ID);
            return FIRST_SEGMENT_ID;
        } else {
            final Pair<K, SegmentId> max = Pair.of(list.lastEntry().getKey(),
                    list.lastEntry().getValue());
            list.remove(max.getKey());
            final Pair<K, SegmentId> newMax = Pair.of(key, max.getValue());
            list.put(newMax.getKey(), newMax.getValue());
            return newMax.getValue();
        }
    }

    public void insertSegment(final K key, final SegmentId segmentId) {
        Objects.requireNonNull(key, "Key can't be null");
        if (list.containsValue(segmentId)) {
            throw new IllegalArgumentException(
                    String.format("Segment id '%s' already exists", segmentId));
        }
        list.put(key, segmentId);
        isDirty = true;
    }

    public Stream<Pair<K, SegmentId>> getSegmentsAsStream() {
        return list.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()));
    }

    public List<SegmentId> getSegmentIds() {
        return getSegmentIds(SegmentWindow.unbounded());
    }

    public List<SegmentId> getSegmentIds(SegmentWindow segmentWindow) {
        return list.entrySet().stream()//
                .skip(segmentWindow.getIntOffset())//
                .limit(segmentWindow.getIntLimit())//
                .map(entry -> entry.getValue())//
                .collect(Collectors.toList());
    }

    public void flush() {
        if (isDirty) {
            try (SortedDataFileWriter<K, SegmentId> writer = sdf.openWriter()) {
                list.forEach((k, v) -> writer.write(Pair.of(k, v)));
            }
        }
        isDirty = false;
    }

    @Override
    public void close() {
        flush();
    }
}
