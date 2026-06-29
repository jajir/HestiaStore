package org.hestiastore.index.segmentindex.routemap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe persistent route-map implementation backed by a read/write lock.
 *
 * @param <K> key type
 */
public final class PersistentSegmentRouteMap<K> extends AbstractCloseableResource
        implements SegmentRouteMap<K> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(PersistentSegmentRouteMap.class);
    private static final SegmentIdTypeDescriptor tdSegId = new SegmentIdTypeDescriptor();

    private static final String FILE_NAME = "index.map";

    /**
     * When new index is created than this is id of first segment.
     */
    private static final SegmentId FIRST_SEGMENT_ID = SegmentId.of(0);

    private TreeMap<K, SegmentId> list;
    private volatile TreeMap<K, SegmentId> snapshot;
    private final SortedDataFile<K, SegmentId> sdf;
    private final Comparator<K> keyComparator;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private boolean isDirty = false;
    private final AtomicLong version = new AtomicLong(0);

    public PersistentSegmentRouteMap(final Directory directoryFacade,
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
    @Override
    public void validateUniqueSegmentIds() {
        readLock.lock();
        try {
            ensureOpen();
            final TreeMap<K, SegmentId> current = snapshot;
            final HashMap<SegmentId, K> seen = new HashMap<>();
            for (final Map.Entry<K, SegmentId> entry : current.entrySet()) {
                final K key = entry.getKey();
                final SegmentId segmentId = entry.getValue();
                final K oldKey = seen.putIfAbsent(segmentId, key);
                if (oldKey == null) {
                    continue;
                }
                LOGGER.error(
                        "Segment id '{}' is used for segment with key '{}' and segment with key '{}'.",
                        segmentId, key, oldKey);
                throw new IllegalStateException(
                        "Unable to load scarce index, sanity check failed.");
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Finds the segment id mapped to the provided key.
     *
     * @param key key to look up
     * @return segment id or {@code null} when not mapped
     */
    @Override
    public SegmentId findSegmentIdForKey(final K key) {
        readLock.lock();
        try {
            ensureOpen();
            Vldtn.requireNonNull(key, "key");
            final Entry<K, SegmentId> entry = localFindSegmentForKey(key,
                    snapshot);
            return entry == null ? null : entry.getValue();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public RouteMapSnapshot<K> snapshot() {
        readLock.lock();
        try {
            return new RouteMapSnapshot<>(snapshot, version.get());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isAtVersion(final long expectedVersion) {
        readLock.lock();
        try {
            return version.get() == expectedVersion;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void extendMaxKeyIfNeeded(final K key) {
        writeLock.lock();
        try {
            ensureOpen();
            Vldtn.requireNonNull(key, "key");
            if (list.isEmpty()) {
                list.put(key, FIRST_SEGMENT_ID);
                refreshSnapshot();
                isDirty = true;
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Inserts a mapping for the provided key, allocating a segment id when
     * needed.
     *
     * @param key key to map
     * @return segment id assigned to the key
     */
    SegmentId insertKeyToSegment(final K key) {
        writeLock.lock();
        try {
            ensureOpen();
            Vldtn.requireNonNull(key, "key");
            final Entry<K, SegmentId> entry = localFindSegmentForKey(key, list);
            if (entry == null) {
                /*
                 * No route exists yet, so bootstrap the first segment.
                 */
                isDirty = true;
                return bootstrapFirstSegment(key);
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
        if (ceilingEntry != null) {
            return Entry.of(ceilingEntry.getKey(), ceilingEntry.getValue());
        }
        final Map.Entry<K, SegmentId> tailEntry = source.lastEntry();
        if (tailEntry == null) {
            return null;
        }
        return Entry.of(tailEntry.getKey(), tailEntry.getValue());
    }

    private SegmentId bootstrapFirstSegment(final K key) {
        list.put(key, FIRST_SEGMENT_ID);
        refreshSnapshot();
        return FIRST_SEGMENT_ID;
    }

    /**
     * Inserts or updates a mapping for the provided key and segment id.
     *
     * @param key       key to map
     * @param segmentId segment id to associate
     */
    void insertSegment(final K key, final SegmentId segmentId) {
        writeLock.lock();
        try {
            ensureOpen();
            Vldtn.requireNonNull(key, "key");
            Vldtn.requireNonNull(segmentId, "segmentId");
            if (isBoundaryKeyBoundToOtherSegment(key, segmentId)) {
                throw new IllegalArgumentException(
                        duplicateBoundaryKeyMessage(key, list.get(key)));
            }
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
        writeLock.lock();
        try {
            ensureOpen();
            Vldtn.requireNonNull(segmentId, "segmentId");
            Vldtn.requireNonNull(newMaxKey, "newMaxKey");
            Map.Entry<K, SegmentId> existing = null;
            for (final Map.Entry<K, SegmentId> entry : list.entrySet()) {
                if (segmentId.equals(entry.getValue())) {
                    existing = entry;
                    break;
                }
            }
            if (isBoundaryKeyBoundToOtherSegment(newMaxKey, segmentId)) {
                throw new IllegalStateException(
                        duplicateBoundaryKeyMessage(newMaxKey,
                                list.get(newMaxKey)));
            }
            if (existing == null) {
                list.put(newMaxKey, segmentId);
            } else {
                list.remove(existing.getKey());
                list.put(newMaxKey, segmentId);
            }
            refreshSnapshot();
            isDirty = true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Removes the mapping for the provided segment id.
     *
     * @param segmentId segment id to remove
     */
    public void removeSegmentRoute(final SegmentId segmentId) {
        writeLock.lock();
        try {
            removeSegment(segmentId);
        } finally {
            writeLock.unlock();
        }
    }

    private void removeSegment(final SegmentId segmentId) {
        ensureOpen();
        Vldtn.requireNonNull(segmentId, "segmentId");
        if (list.isEmpty()) {
            return;
        }
        boolean removed = false;
        final Iterator<Map.Entry<K, SegmentId>> iterator = list.entrySet()
                .iterator();
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
    }

    @Override
    public boolean tryReplaceRouteWithSplit(final RouteSplitPlan<K> split) {
        writeLock.lock();
        try {
            ensureOpen();
            Vldtn.requireNonNull(split, "split");
            final SegmentId replacedSegmentId = split.getReplacedSegmentId();
            final SegmentId lowerSegmentId = split.getLowerSegmentId();
            final RouteBoundary<K> replacedBoundary = findRouteBoundary(
                    replacedSegmentId);
            if (replacedBoundary == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Route split publish rejected because replaced segment route is missing: replacedSegmentId='{}', lowerSegmentId='{}', lowerMaxKey='{}', upperSegmentId='{}'.",
                            replacedSegmentId, lowerSegmentId,
                            split.getLowerMaxKey(), split.getUpperSegmentId());
                }
                return false;
            }
            validateSplitSegmentIds(split);
            final K upperBoundary = upperBoundaryForSplit(split,
                    replacedBoundary);
            validateSplitBoundaries(split.getLowerMaxKey(), upperBoundary);
            validateSplitBoundaryKeys(split, replacedBoundary, upperBoundary);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "Split debug: map apply replacedSegmentId='{}', upperBoundary='{}', lowerSegmentId='{}', lowerMaxKey='{}', upperSegmentId='{}'.",
                        replacedSegmentId, upperBoundary, lowerSegmentId,
                        split.getLowerMaxKey(), split.getUpperSegmentId());
            }
            applyRouteSplit(replacedBoundary.key(), split, upperBoundary);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private RouteBoundary<K> findRouteBoundary(final SegmentId segmentId) {
        final Map.Entry<K, SegmentId> tailEntry = list.lastEntry();
        for (final Map.Entry<K, SegmentId> entry : list.entrySet()) {
            if (segmentId.equals(entry.getValue())) {
                return new RouteBoundary<>(entry.getKey(),
                        isTailEntry(entry, tailEntry));
            }
        }
        return null;
    }

    private boolean isTailEntry(final Map.Entry<K, SegmentId> entry,
            final Map.Entry<K, SegmentId> tailEntry) {
        return tailEntry != null
                && keyComparator.compare(entry.getKey(), tailEntry.getKey())
                        == 0;
    }

    private void validateSplitSegmentIds(final RouteSplitPlan<K> split) {
        if (split.getLowerSegmentId().equals(split.getUpperSegmentId())) {
            throw new IllegalArgumentException(String.format(
                    "Split child segment id '%s' is used for both routes.",
                    split.getLowerSegmentId()));
        }
        validateNewSplitSegmentId(split.getLowerSegmentId(),
                split.getReplacedSegmentId());
        validateNewSplitSegmentId(split.getUpperSegmentId(),
                split.getReplacedSegmentId());
    }

    private void validateNewSplitSegmentId(final SegmentId segmentId,
            final SegmentId replacedSegmentId) {
        if (segmentId.equals(replacedSegmentId)
                || !list.containsValue(segmentId)) {
            return;
        }
        throw new IllegalArgumentException(
                String.format("Segment id '%s' already exists", segmentId));
    }

    private K upperBoundaryForSplit(final RouteSplitPlan<K> split,
            final RouteBoundary<K> replacedBoundary) {
        if (!replacedBoundary.tail()) {
            return replacedBoundary.key();
        }
        return split.getUpperMaxKey().orElseThrow(
                () -> new IllegalArgumentException(String.format(
                        "Tail route split for segment '%s' requires upperMaxKey.",
                        split.getReplacedSegmentId())));
    }

    private void validateSplitBoundaries(final K lowerBoundary,
            final K upperBoundary) {
        if (keyComparator.compare(lowerBoundary, upperBoundary) < 0) {
            return;
        }
        throw new IllegalArgumentException(String.format(
                "Split lower max key '%s' must be smaller than upper boundary '%s'.",
                lowerBoundary, upperBoundary));
    }

    private void validateSplitBoundaryKeys(final RouteSplitPlan<K> split,
            final RouteBoundary<K> replacedBoundary, final K upperBoundary) {
        validateSplitBoundaryKey(split.getLowerMaxKey(),
                replacedBoundary.key());
        validateSplitBoundaryKey(upperBoundary, replacedBoundary.key());
    }

    private void validateSplitBoundaryKey(final K boundary,
            final K replacedBoundary) {
        if (!list.containsKey(boundary)
                || keyComparator.compare(boundary, replacedBoundary) == 0) {
            return;
        }
        throw new IllegalArgumentException(
                duplicateBoundaryKeyMessage(boundary, list.get(boundary)));
    }

    private boolean isBoundaryKeyBoundToOtherSegment(final K key,
            final SegmentId segmentId) {
        return list.containsKey(key) && !segmentId.equals(list.get(key));
    }

    private String duplicateBoundaryKeyMessage(final K key,
            final SegmentId segmentId) {
        return String.format(
                "Segment max key '%s' is already bound to segment '%s'", key,
                segmentId);
    }

    private void applyRouteSplit(final K replacedBoundary,
            final RouteSplitPlan<K> split, final K upperBoundary) {
        list.remove(replacedBoundary);
        list.put(split.getLowerMaxKey(), split.getLowerSegmentId());
        list.put(upperBoundary, split.getUpperSegmentId());
        refreshSnapshot();
        isDirty = true;
    }

    /**
     * Returns the segment ids in key order.
     *
     * @return ordered list of segment ids
     */
    @Override
    public List<SegmentId> getSegmentIds() {
        return getSegmentIds(SegmentWindow.unbounded());
    }

    /**
     * Returns the segment ids within the provided window.
     *
     * @param segmentWindow window to apply
     * @return ordered list of segment ids
     */
    @Override
    public List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        readLock.lock();
        try {
            return snapshot().getSegmentIds(segmentWindow);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Flushes all changes to disk if there are any. This method is
     * automatically called when this cache is closed.
     */
    @Override
    public void flushIfDirty() {
        writeLock.lock();
        try {
            ensureOpen();
            persistIfDirty();
        } finally {
            writeLock.unlock();
        }
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
        writeLock.lock();
        try {
            persistIfDirty();
        } finally {
            writeLock.unlock();
        }
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

    private static final class RouteBoundary<T> {

        private final T key;
        private final boolean tail;

        private RouteBoundary(final T key, final boolean tail) {
            this.key = key;
            this.tail = tail;
        }

        private T key() {
            return key;
        }

        private boolean tail() {
            return tail;
        }
    }

}
