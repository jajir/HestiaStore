package org.hestiastore.index.segmentindex.mapping;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.split.RouteSplitPlan;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent non-thread-safe implementation backing
 * {@link KeyToSegmentMapSynchronizedAdapter}.
 *
 * @param <K> key type
 */
public final class KeyToSegmentMapImpl<K> extends AbstractCloseableResource {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final TypeDescriptorSegmentId tdSegId = new TypeDescriptorSegmentId();

    private static final String FILE_NAME = "index.map";

    /**
     * When new index is created than this is id of first segment.
     */
    private static final SegmentId FIRST_SEGMENT_ID = SegmentId.of(0);

    private TreeMap<K, SegmentId> list;
    private volatile TreeMap<K, SegmentId> snapshot;
    private final SortedDataFile<K, SegmentId> sdf;
    private final Comparator<K> keyComparator;
    private boolean isDirty = false;
    private final AtomicLong version = new AtomicLong(0);

    public KeyToSegmentMapImpl(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = Vldtn.requireNonNull(
                keyTypeDescriptor.getComparator(),
                "keyTypeDescriptor.getComparator()");
        this.sdf = SortedDataFile.<K, SegmentId>builder() //
                .withDirectory(directoryFacade) //
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
        validateUniqueSegmentIds();
    }

    /**
     * Verify, that all segment ids are unique.
     */
    void validateUniqueSegmentIds() {
        ensureOpen();
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

    /**
     * Finds the segment id mapped to the provided key.
     *
     * @param key key to look up
     * @return segment id or {@code null} when not mapped
     */
    SegmentId findSegmentIdForKey(final K key) {
        ensureOpen();
        Vldtn.requireNonNull(key, "key");
        final Entry<K, SegmentId> entry = localFindSegmentForKey(key, snapshot);
        return entry == null ? null : entry.getValue();
    }

    Snapshot<K> snapshot() {
        return new Snapshot<>(snapshot, version.get());
    }

    boolean isAtVersion(final long expectedVersion) {
        return version.get() == expectedVersion;
    }

    boolean isSnapshotVersionCurrent(final long expectedVersion) {
        return version.get() == expectedVersion;
    }

    boolean extendMaxKeyIfNeeded(final K key) {
        Vldtn.requireNonNull(key, "key");
        if (list.isEmpty()) {
            list.put(key, FIRST_SEGMENT_ID);
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
    }

    /**
     * Inserts a mapping for the provided key, allocating a segment id when
     * needed.
     *
     * @param key key to map
     * @return segment id assigned to the key
     */
    SegmentId insertKeyToSegment(final K key) {
        ensureOpen();
        Vldtn.requireNonNull(key, "key");
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

    /**
     * Inserts or updates a mapping for the provided key and segment id.
     *
     * @param key       key to map
     * @param segmentId segment id to associate
     */
    void insertSegment(final K key, final SegmentId segmentId) {
        ensureOpen();
        Vldtn.requireNonNull(key, "key");
        if (list.containsValue(segmentId)) {
            throw new IllegalArgumentException(
                    String.format("Segment id '%s' already exists", segmentId));
        }
        list.put(key, segmentId);
        refreshSnapshot();
        isDirty = true;
    }

    void updateSegmentMaxKey(final SegmentId segmentId, final K newMaxKey) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(newMaxKey, "newMaxKey");
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
    }

    /**
     * Removes the mapping for the provided segment id.
     *
     * @param segmentId segment id to remove
     */
    public void removeSegmentRoute(final SegmentId segmentId) {
        removeSegmentAndReturnMaxKey(segmentId);
    }

    private K removeSegmentAndReturnMaxKey(final SegmentId segmentId) {
        ensureOpen();
        Vldtn.requireNonNull(segmentId, "segmentId");
        if (list.isEmpty()) {
            return null;
        }
        boolean removed = false;
        K removedKey = null;
        final java.util.Iterator<Map.Entry<K, SegmentId>> iterator = list
                .entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<K, SegmentId> entry = iterator.next();
            if (segmentId.equals(entry.getValue())) {
                final K keyToRemove = entry.getKey();
                iterator.remove();
                removedKey = keyToRemove;
                removed = true;
            }
        }
        if (removed) {
            refreshSnapshot();
            isDirty = true;
        }
        return removedKey;
    }

    boolean tryApplySplitPlan(final RouteSplitPlan<K> plan) {
        Vldtn.requireNonNull(plan, "plan");
        final SegmentId replacedSegmentId = plan.getReplacedSegmentId();
        final SegmentId lowerSegmentId = plan.getLowerSegmentId();
        final K upperMaxKey = removeSegmentAndReturnMaxKey(replacedSegmentId);
        if (upperMaxKey == null) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Split debug: map apply replacedSegmentId='{}', oldMaxKey='{}', lowerSegmentId='{}', lowerMaxKey='{}', splitMode='{}', upperSegmentId='{}'.",
                    replacedSegmentId, upperMaxKey, lowerSegmentId,
                    plan.getLowerMaxKey(), plan.getSplitMode(),
                    plan.getUpperSegmentId().orElse(null));
        }
        insertSegment(plan.getLowerMaxKey(), lowerSegmentId);
        if (plan.isSplit()) {
            final SegmentId upperSegmentId = Vldtn.requireNonNull(
                    plan.getUpperSegmentId().orElse(null), "upperSegmentId");
            insertSegment(upperMaxKey, upperSegmentId);
        }
        return true;
    }

    /**
     * Returns the segment ids in key order.
     *
     * @return ordered list of segment ids
     */
    public List<SegmentId> getSegmentIds() {
        ensureOpen();
        return getSegmentIds(SegmentWindow.unbounded());
    }

    /**
     * Returns the segment ids within the provided window.
     *
     * @param segmentWindow window to apply
     * @return ordered list of segment ids
     */
    List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        ensureOpen();
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
    public void flushIfDirty() {
        ensureOpen();
        persistIfDirty();
    }

    private void persistIfDirty() {
        if (isDirty) {
            sdf.openWriterTx().execute(writer -> {
                list.forEach((k, v) -> writer.write(Entry.of(k, v)));
            });
        }
        isDirty = false;
    }

    @Override
    protected void doClose() {
        persistIfDirty();
    }

    private void refreshSnapshot() {
        snapshot = new TreeMap<>(list);
        version.incrementAndGet();
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " already closed");
        }
    }

}
