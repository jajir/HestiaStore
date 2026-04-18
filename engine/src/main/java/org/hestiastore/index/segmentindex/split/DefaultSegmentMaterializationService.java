package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Default implementation of offline segment materialization for route splits.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class DefaultSegmentMaterializationService<K, V>
        implements SegmentMaterializationService<K, V> {

    private final SegmentMaterializationFileSystem fileSystem;
    private final SegmentRegistry.Materialization<K, V> materialization;

    /**
     * Creates a materialization service backed by the provided collaborators.
     *
     * @param directoryFacade root directory for segment storage
     * @param materialization registry materialization view used to allocate ids
     *                        and open synchronous segment writers
     */
    public DefaultSegmentMaterializationService(
            final Directory directoryFacade,
            final SegmentRegistry.Materialization<K, V> materialization) {
        this.fileSystem = new SegmentMaterializationFileSystem(
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"));
        this.materialization = Vldtn.requireNonNull(materialization,
                "materialization");
    }

    @Override
    public PreparedSegmentHandle<K, V> openPreparedSegment() {
        final SegmentId segmentId = Vldtn.requireNonNull(
                materialization.nextSegmentId(), "segmentId");
        fileSystem.ensureSegmentDirectory(segmentId);
        try {
            return new DefaultPreparedSegmentHandle<>(segmentId,
                    materialization.openWriterTx(segmentId), fileSystem);
        } catch (final RuntimeException ex) {
            fileSystem.deletePreparedSegment(segmentId);
            throw ex;
        }
    }

    @Override
    public void deletePreparedSegment(final SegmentId segmentId) {
        fileSystem.deletePreparedSegment(segmentId);
    }
}
