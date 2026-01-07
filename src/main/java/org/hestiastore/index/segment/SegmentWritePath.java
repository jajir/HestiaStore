package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

final class SegmentWritePath<K, V> {

    private final SegmentCache<K, V> segmentCache;
    private final VersionController versionController;

    SegmentWritePath(final SegmentCache<K, V> segmentCache,
            final VersionController versionController) {
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
    }

    void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        // TODO make sure that'scorrect behavior to kill all iterators on put
        versionController.changeVersion();
        segmentCache.putToWriteCache(Entry.of(key, value));
    }

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

    int getNumberOfKeysInWriteCache() {
        return segmentCache.getNumberOfKeysInWriteCache();
    }

    List<Entry<K, V>> freezeWriteCacheForFlush() {
        final boolean hadFrozen = segmentCache.hasFrozenWriteCache();
        final List<Entry<K, V>> entries = segmentCache.freezeWriteCache();
        if (!entries.isEmpty() && !hadFrozen) {
            versionController.changeVersion();
        }
        return entries;
    }

    void applyFrozenWriteCacheAfterFlush() {
        segmentCache.mergeFrozenWriteCacheToDeltaCache();
    }
}
