package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
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
    private volatile TreeMap<K, SegmentId> snapshot;
    private final SortedDataFile<K, SegmentId> sdf;
    private final Comparator<K> keyComparator;
    private boolean isDirty = false;
    private final AtomicInteger nextSegmentId;
    private final AtomicLong version = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock writeLock = lock.writeLock();

    KeySegmentCache(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = Vldtn.requireNonNull(
                keyTypeDescriptor.getComparator(),
                "keyTypeDescriptor.getComparator()");
        this.sdf = SortedDataFile.<K, SegmentId>builder() //
                .withAsyncDirectory(directoryFacade) //
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
        this.snapshot = new TreeMap<>(list);
        checkUniqueSegmentIds();
        this.nextSegmentId = new AtomicInteger(
                list.values().stream().mapToInt(SegmentId::getId).max()
                        .orElse(-1) + 1);
    }

    /**
     * Verify, that all segment ids are unique.
     */
    public void checkUniqueSegmentIds() {
        final TreeMap<K, SegmentId> current = snapshot;
        final HashMap<SegmentId, K> tmp = new HashMap<SegmentId, K>();
        final AtomicBoolean fail = new AtomicBoolean(false);
        current.forEach((key, segmentId) -> {
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
        Vldtn.requireNonNull(key, "key");
        final Entry<K, SegmentId> entry = localFindSegmentForKey(key, snapshot);
        return entry == null ? null : entry.getValue();
    }

    Snapshot<K> snapshot() {
        return new Snapshot<>(snapshot, version.get());
    }

    boolean isMappingValid(final K key, final SegmentId expectedSegmentId,
            final long expectedVersion) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(expectedSegmentId, "expectedSegmentId");
        return version.get() == expectedVersion;
    }

    boolean isKeyMappedToSegment(final K key,
            final SegmentId expectedSegmentId) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(expectedSegmentId, "expectedSegmentId");
        final SegmentId current = findSegmentId(key);
        return expectedSegmentId.equals(current);
    }

    public SegmentId findNewSegmentId() {
        return SegmentId.of(nextSegmentId.getAndIncrement());
    }

    boolean tryExtendMaxKey(final K key, final Snapshot<K> snapshot) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(snapshot, "snapshot");
        writeLock.lock();
        try {
            if (version.get() != snapshot.version()) {
                return false;
            }
            if (list.isEmpty()) {
                list.put(key, FIRST_SEGMENT_ID);
                nextSegmentId.updateAndGet(
                        current -> Math.max(current,
                                FIRST_SEGMENT_ID.getId() + 1));
                refreshSnapshot();
                isDirty = true;
                return true;
            }
            final Map.Entry<K, SegmentId> lastEntry = list.lastEntry();
            if (keyComparator.compare(key, lastEntry.getKey()) > 0) {
                list.remove(lastEntry.getKey());
                list.put(key, lastEntry.getValue());
                refreshSnapshot();
                isDirty = true;
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public SegmentId insertKeyToSegment(final K key) {
        Vldtn.requireNonNull(key, "key");
        writeLock.lock();
        try {
            final Entry<K, SegmentId> entry = localFindSegmentForKey(key, list);
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

    private Entry<K, SegmentId> localFindSegmentForKey(final K key,
            final TreeMap<K, SegmentId> source) {
        Vldtn.requireNonNull(key, "key");
        final Map.Entry<K, SegmentId> ceilingEntry = source.ceilingEntry(key);
        if (ceilingEntry == null) {
            return null;
        } else {
            return Entry.of(ceilingEntry.getKey(), ceilingEntry.getValue());
        }
    }

    private SegmentId updateMaxKey(final K key) {
        if (list.size() == 0) {
            list.put(key, FIRST_SEGMENT_ID);
            refreshSnapshot();
            nextSegmentId.updateAndGet(
                    current -> Math.max(current, FIRST_SEGMENT_ID.getId() + 1));
            return FIRST_SEGMENT_ID;
        } else {
            final Entry<K, SegmentId> max = Entry.of(list.lastEntry().getKey(),
                    list.lastEntry().getValue());
            list.remove(max.getKey());
            final Entry<K, SegmentId> newMax = Entry.of(key, max.getValue());
            list.put(newMax.getKey(), newMax.getValue());
            refreshSnapshot();
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
            refreshSnapshot();
            isDirty = true;
        } finally {
            writeLock.unlock();
        }
    }

    void updateSegmentMaxKey(final SegmentId segmentId, final K newMaxKey) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(newMaxKey, "newMaxKey");
        writeLock.lock();
        try {
            Map.Entry<K, SegmentId> existing = null;
            for (final Map.Entry<K, SegmentId> entry : list.entrySet()) {
                if (segmentId.equals(entry.getValue())) {
                    existing = entry;
                    break;
                }
            }
            if (existing == null) {
                if (list.containsKey(newMaxKey)
                        && !segmentId.equals(list.get(newMaxKey))) {
                    throw new IllegalStateException(String.format(
                            "Segment max key '%s' is already bound to segment '%s'",
                            newMaxKey, list.get(newMaxKey)));
                }
                list.put(newMaxKey, segmentId);
                refreshSnapshot();
                isDirty = true;
                return;
            }
            if (list.containsKey(newMaxKey)
                    && !segmentId.equals(list.get(newMaxKey))) {
                throw new IllegalStateException(String.format(
                        "Segment max key '%s' is already bound to segment '%s'",
                        newMaxKey, list.get(newMaxKey)));
            }
            list.remove(existing.getKey());
            list.put(newMaxKey, segmentId);
            refreshSnapshot();
            isDirty = true;
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        writeLock.lock();
        try {
            if (list.isEmpty()) {
                return;
            }
            boolean removed = false;
            final java.util.Iterator<Map.Entry<K, SegmentId>> iterator = list
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<K, SegmentId> entry = iterator.next();
                if (segmentId.equals(entry.getValue())) {
                    iterator.remove();
                    removed = true;
                }
            }
            if (removed) {
                refreshSnapshot();
                isDirty = true;
            }
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
        return snapshot.entrySet().stream()//
                .skip(segmentWindow.getIntOffset())//
                .limit(segmentWindow.getIntLimit())//
                .map(entry -> entry.getValue())//
                .toList();
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
        if (snapshot.isEmpty()) {
            return List.of();
        }
        final List<Entry<K, SegmentId>> out = new ArrayList<>(snapshot.size());
        snapshot.forEach((key, segmentId) -> out.add(Entry.of(key, segmentId)));
        return out;
    }

    private void refreshSnapshot() {
        snapshot = new TreeMap<>(list);
        version.incrementAndGet();
    }

    static final class Snapshot<K> {
        private final TreeMap<K, SegmentId> map;
        private final long version;

        private Snapshot(final TreeMap<K, SegmentId> map,
                final long version) {
            this.map = map;
            this.version = version;
        }

        SegmentId findSegmentId(final K key) {
            Vldtn.requireNonNull(key, "key");
            final Map.Entry<K, SegmentId> ceilingEntry = map.ceilingEntry(key);
            return ceilingEntry == null ? null : ceilingEntry.getValue();
        }

        long version() {
            return version;
        }
    }
}
