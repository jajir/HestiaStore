package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentIdAllocator;

/**
 * Default implementation of offline segment materialization for route splits.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class DefaultSegmentMaterializationService<K, V>
        implements SegmentMaterializationService<K, V> {

    private final SegmentIdAllocator segmentIdAllocator;
    private final SegmentMaterializationFileSystem fileSystem;
    private final SegmentFactory<K, V> segmentFactory;

    /**
     * Creates a materialization service backed by the provided collaborators.
     *
     * @param segmentIdAllocator allocator for new segment ids
     * @param directoryFacade root directory for segment storage
     * @param segmentFactory factory used to open synchronous segment writers
     */
    public DefaultSegmentMaterializationService(
            final SegmentIdAllocator segmentIdAllocator,
            final Directory directoryFacade,
            final SegmentFactory<K, V> segmentFactory) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.fileSystem = new SegmentMaterializationFileSystem(
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"));
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
    }

    @Override
    public PreparedSegmentHandle<K, V> openPreparedSegment() {
        final SegmentId segmentId = Vldtn.requireNonNull(
                segmentIdAllocator.nextId(), "segmentId");
        fileSystem.ensureSegmentDirectory(segmentId);
        try {
            return new DefaultPreparedSegmentHandle<>(segmentId,
                    segmentFactory.openWriterTx(segmentId), fileSystem);
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
