package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Minimal contract for retrieving and managing segments from a registry.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentRegistry<K, V> {

    /**
     * Creates a builder for registry instances with required inputs.
     *
     * @param <M> key type
     * @param <N> value type
     * @param directoryFacade base directory for segments
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf index configuration
     * @param maintenanceExecutor maintenance executor for segments
     * @return registry builder
     */
    static <M, N> SegmentRegistryBuilder<M, N> builder(
            final AsyncDirectory directoryFacade,
            final TypeDescriptor<M> keyTypeDescriptor,
            final TypeDescriptor<N> valueTypeDescriptor,
            final IndexConfiguration<M, N> conf,
            final ExecutorService maintenanceExecutor) {
        return new SegmentRegistryBuilder<>(directoryFacade, keyTypeDescriptor,
                valueTypeDescriptor, conf, maintenanceExecutor);
    }

    /**
     * Returns the segment for the provided id, loading it if needed.
     *
     * @param segmentId segment id to load
     * @return registry result containing the segment or a status
     */
    SegmentRegistryResult<Segment<K, V>> getSegment(SegmentId segmentId);

    /**
     * Allocates a new, unused segment id.
     *
     * @return registry result containing the new segment id or a status
     */
    SegmentRegistryResult<SegmentId> allocateSegmentId();

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     */
    default SegmentRegistryResult<Segment<K, V>> createSegment() {
        final SegmentRegistryResult<SegmentId> idResult = allocateSegmentId();
        if (idResult.getStatus() == SegmentRegistryResultStatus.OK) {
            return getSegment(idResult.getValue());
        }
        if (idResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (idResult.getStatus() == SegmentRegistryResultStatus.ERROR) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.busy();
    }

    /**
     * Removes a segment from the registry, closing and deleting its files.
     *
     * @param segmentId segment id to remove
     * @return registry result status
     */
    SegmentRegistryResult<Void> deleteSegment(SegmentId segmentId);

    /**
     * Closes the registry, releasing cached segments and executors.
     *
     * @return registry result status
     */
    SegmentRegistryResult<Void> close();
}
