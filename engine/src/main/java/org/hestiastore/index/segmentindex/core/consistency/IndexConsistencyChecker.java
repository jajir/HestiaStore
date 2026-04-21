package org.hestiastore.index.segmentindex.core.consistency;

import java.util.function.Predicate;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterate through all segments in index. It verify data describing structures
 * and data itself are consistent.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class IndexConsistencyChecker<K, V> {
    private static final String ERROR_MSG = "Index is broken. "
            + "File 'index.map' containing information about segments is corrupted. ";
    private static final IndexRetryPolicy DEFAULT_RETRY_POLICY = new IndexRetryPolicy(
            IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS,
            IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS);
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentRegistry<K, V> segmentRegistry;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final Predicate<SegmentId> segmentFilter;
    private final IndexRetryPolicy retryPolicy;

    public IndexConsistencyChecker(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this(keyToSegmentMap, segmentRegistry, keyTypeDescriptor,
                segmentId -> true, DEFAULT_RETRY_POLICY);
    }

    public IndexConsistencyChecker(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final TypeDescriptor<K> keyTypeDescriptor,
            final Predicate<SegmentId> segmentFilter) {
        this(keyToSegmentMap, segmentRegistry, keyTypeDescriptor,
                segmentFilter, DEFAULT_RETRY_POLICY);
    }

    IndexConsistencyChecker(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final TypeDescriptor<K> keyTypeDescriptor,
            final Predicate<SegmentId> segmentFilter,
            final IndexRetryPolicy retryPolicy) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.segmentFilter = Vldtn.requireNonNull(segmentFilter,
                "segmentFilter");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Scans all segments and verifies map-to-segment consistency, attempting to
     * repair obvious corruption when possible.
     */
    public void checkAndRepairConsistency() {
        final Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        snapshot.getSegmentIds(SegmentWindow.unbounded()).forEach(segmentId -> {
            if (segmentId == null) {
                throw new IndexException(ERROR_MSG + "Segment id is null.");
            }
            if (!segmentFilter.test(segmentId)) {
                logger.debug("Skipping consistency check for segment '{}'.",
                        segmentId);
                return;
            }
            logger.debug("checking segment '{}'.", segmentId);
            final SegmentHandle<K, V> segment = awaitLoadedSegment(segmentId);
            final K maxKey = segment.checkAndRepairConsistency();
            if (maxKey == null) {
                removeEmptySegment(segmentId, segment);
                return;
            }
            final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(
                    maxKey);
            if (!segmentId.equals(routedSegmentId)) {
                throw new IndexException(String.format(ERROR_MSG
                        + "Segment '%s' contains max key '%s', which routes to segment '%s' in the index map.",
                        segmentId, maxKey, routedSegmentId));
            }
            logger.debug("Checking segment '{}' id done.", segmentId);
        });
    }

    private void removeEmptySegment(final SegmentId segmentId,
            final SegmentHandle<K, V> segment) {
        if (!confirmEmptyUnderIsolation(segment)) {
            return;
        }
        logger.warn("Segment '{}' is empty. Removing it from index map.",
                segmentId);
        keyToSegmentMap.removeSegmentRoute(segmentId);
        keyToSegmentMap.flushIfDirty();
        segmentRegistry.deleteSegment(segmentId);
    }

    private boolean confirmEmptyUnderIsolation(
            final SegmentHandle<K, V> segment) {
        try (EntryIterator<K, V> iterator = segment
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION)) {
            return !iterator.hasNext();
        }
    }

    private SegmentHandle<K, V> awaitLoadedSegment(final SegmentId segmentId) {
        try {
            return segmentRegistry.loadSegment(segmentId);
        } catch (final IndexException e) {
            throw new IndexException(String.format(
                    ERROR_MSG + "Segment '%s' is not found in index.",
                    segmentId), e);
        }
    }

}
