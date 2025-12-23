package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class holds and maintain map<key, segmentId>. Key is max in in given segment.
 * 
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
public final class KeySegmentCache<K> extends AbstractCloseableResource {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final TypeDescriptorSegmentId tdSegId = new TypeDescriptorSegmentId();

    private static final String FILE_NAME = "index.map";

    /**
     * When new index is created than this is id of first segment.
     */
    public static final SegmentId FIRST_SEGMENT_ID = SegmentId.of(0);

    private TreeMap<K, SegmentId> list;
    private final SortedDataFile<K, SegmentId> sdf;
    private final Comparator<K> keyComparator;
    private boolean isDirty = false;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    KeySegmentCache(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor) {
        Vldtn.requireNonNull(directory, "directory");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = Vldtn.requireNonNull(
                keyTypeDescriptor.getComparator(),
                "keyTypeDescriptor.getComparator()");
        this.sdf = SortedDataFile.<K, SegmentId>builder() //
                .withDirectory(directory) //
                .withFileName(FILE_NAME)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(tdSegId) //
                .build();

        this.list = new TreeMap<>(keyComparator);
        try (EntryIterator<K, SegmentId> reader = sdf.openIterator()) {
            while (reader.hasNext()) {
                final Entry<K, SegmentId> entry = reader.next();
                list.put(entry.getKey(), entry.getValue());
            }
        }
        checkUniqueSegmentIds();
    }

    /**
     * Verify, that all segment ids are unique.
     */
    public void checkUniqueSegmentIds() {
        readLock.lock();
        try {
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
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId findSegmentId(final K key) {
        Vldtn.requireNonNull(key, "key");
        readLock.lock();
        try {
            final Entry<K, SegmentId> entry = localFindSegmentForKey(key);
            return entry == null ? null : entry.getValue();
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId findNewSegmentId() {
        readLock.lock();
        try {
            if (list.isEmpty()) {
                return SegmentId.of(0);
            }
            int maxId = Integer.MIN_VALUE;
            for (SegmentId sid : list.values()) {
                if (sid.getId() > maxId) {
                    maxId = sid.getId();
                }
            }
            return SegmentId.of(maxId + 1);
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId insertKeyToSegment(final K key) {
        Vldtn.requireNonNull(key, "key");
        writeLock.lock();
        try {
            final Entry<K, SegmentId> entry = localFindSegmentForKey(key);
            if (entry == null) {
                /*
                 * Key is bigger that all key so it will at last segment. But key at
                 * last segment is smaller than adding one. Because of that key have
                 * to be upgraded.
                 */
                isDirty = true;
                return updateMaxKey(key);
            } else {
                return entry.getValue();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private Entry<K, SegmentId> localFindSegmentForKey(final K key) {
        Vldtn.requireNonNull(key, "key");
        final Map.Entry<K, SegmentId> ceilingEntry = list.ceilingEntry(key);
        if (ceilingEntry == null) {
            return null;
        } else {
            return Entry.of(ceilingEntry.getKey(), ceilingEntry.getValue());
        }
    }

    private SegmentId updateMaxKey(final K key) {
        if (list.size() == 0) {
            list.put(key, FIRST_SEGMENT_ID);
            return FIRST_SEGMENT_ID;
        } else {
            final Entry<K, SegmentId> max = Entry.of(list.lastEntry().getKey(),
                    list.lastEntry().getValue());
            list.remove(max.getKey());
            final Entry<K, SegmentId> newMax = Entry.of(key, max.getValue());
            list.put(newMax.getKey(), newMax.getValue());
            return newMax.getValue();
        }
    }

    public void insertSegment(final K key, final SegmentId segmentId) {
        Vldtn.requireNonNull(key, "key");
        writeLock.lock();
        try {
            if (list.containsValue(segmentId)) {
                throw new IllegalArgumentException(String.format(
                        "Segment id '%s' already exists", segmentId));
            }
            list.put(key, segmentId);
            isDirty = true;
        } finally {
            writeLock.unlock();
        }
    }

    public Stream<Entry<K, SegmentId>> getSegmentsAsStream() {
        return snapshotSegments().stream();
    }

    public List<SegmentId> getSegmentIds() {
        return getSegmentIds(SegmentWindow.unbounded());
    }

    public List<SegmentId> getSegmentIds(SegmentWindow segmentWindow) {
        readLock.lock();
        try {
            return list.entrySet().stream()//
                    .skip(segmentWindow.getIntOffset())//
                    .limit(segmentWindow.getIntLimit())//
                    .map(entry -> entry.getValue())//
                    .toList();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Flushes all changes to disk if there are any. This method is
     * automatically called when this cache is closed.
     */
    public void optionalyFlush() {
        writeLock.lock();
        try {
            if (isDirty) {
                sdf.openWriterTx().execute(writer -> {
                    list.forEach((k, v) -> writer.write(Entry.of(k, v)));
                });
            }
            isDirty = false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected void doClose() {
        optionalyFlush();
    }

    private List<Entry<K, SegmentId>> snapshotSegments() {
        readLock.lock();
        try {
            if (list.isEmpty()) {
                return List.of();
            }
            final List<Entry<K, SegmentId>> out = new ArrayList<>(list.size());
            list.forEach((key, segmentId) -> out.add(Entry.of(key, segmentId)));
            return out;
        } finally {
            readLock.unlock();
        }
    }
}
