package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Encapsulates write-cache mutations and version tracking for a segment.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentWritePath<K, V> {

    private final SegmentCache<K, V> segmentCache;
    private final VersionController versionController;

    SegmentWritePath(final SegmentCache<K, V> segmentCache,
            final VersionController versionController) {
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
    }

    /**
     * Writes an entry into the write cache and increments the version.
     *
     * @param key key to store
     * @param value value to store
     */
    void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        // TODO make sure that'scorrect behavior to kill all iterators on put
        versionController.changeVersion();
        segmentCache.putToWriteCache(Entry.of(key, value));
    }

    /**
     * Attempts a non-blocking write into the write cache.
     *
     * @param key key to store
     * @param value value to store
     * @return true when the write cache accepted the entry
     */
    boolean tryPutWithoutWaiting(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        if (segmentCache.tryPutToWriteCacheWithoutWaiting(
                Entry.of(key, value))) {
            versionController.changeVersion();
            return true;
        }
        return false;
    }

    void awaitWriteCapacity() {
        segmentCache.awaitWriteCapacity();
    }

    /**
     * Returns the current number of buffered write-cache keys.
     *
     * @return number of keys in write cache
     */
    int getNumberOfKeysInWriteCache() {
        return segmentCache.getNumberOfKeysInWriteCache();
    }

    /**
     * Freezes the write cache into a flushable snapshot.
     *
     * @return snapshot entries in sorted order
     */
    List<Entry<K, V>> freezeWriteCacheForFlush() {
        final boolean hadFrozen = segmentCache.hasFrozenWriteCache();
        final List<Entry<K, V>> entries = segmentCache.freezeWriteCache();
        if (!entries.isEmpty() && !hadFrozen) {
            versionController.changeVersion();
        }
        return entries;
    }

    /**
     * Merges the frozen write cache into the delta cache after flush.
     */
    void applyFrozenWriteCacheAfterFlush() {
        segmentCache.mergeFrozenWriteCacheToDeltaCache();
    }
}
