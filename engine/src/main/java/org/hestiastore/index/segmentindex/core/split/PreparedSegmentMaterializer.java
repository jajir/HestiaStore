package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Default implementation of offline segment materialization for route splits.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class PreparedSegmentMaterializer<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final Directory directoryFacade;
    private final SegmentRegistry<K, V> segmentRegistry;

    /**
     * Creates a materialization service backed by the provided collaborators.
     *
     * @param directoryFacade root directory for segment storage
     * @param segmentRegistry registry used to materialize and delete prepared
     *                        segments
     */
    PreparedSegmentMaterializer(
            final Directory directoryFacade,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    /**
     * Materializes a prepared split in one parent iterator pass.
     * <p>
     * Prepared child files are committed only after both children satisfy the
     * configured minimum size. Deterministic undersized-child outcomes discard
     * all prepared files and ask the caller to compact the parent instead.
     *
     * @param parentSegment parent segment being split
     * @param targetLowerCount estimated lower-child cut point
     * @param minKeysPerChildSegment minimum live keys required in each child
     * @param iterator isolated parent snapshot iterator
     * @return preparation outcome
     */
    RouteSplitPreparation<K> materializeRouteSplit(
            final Segment<K, V> parentSegment,
            final long targetLowerCount,
            final long minKeysPerChildSegment,
            final EntryIterator<K, V> iterator) {
        Vldtn.requireNonNull(parentSegment, "parentSegment");
        Vldtn.requireNonNull(iterator, "iterator");
        final long validatedTargetLowerCount = requireAtLeastOne(
                targetLowerCount, "targetLowerCount");
        final long validatedMinKeysPerChildSegment = requireAtLeastOne(
                minKeysPerChildSegment, "minKeysPerChildSegment");
        SegmentId lowerSegmentId = null;
        WriteTransaction<K, V> lowerWriterTx = null;
        EntryWriter<K, V> lowerWriter = null;
        SegmentId upperSegmentId = null;
        WriteTransaction<K, V> upperWriterTx = null;
        EntryWriter<K, V> upperWriter = null;
        boolean materializationCompleted = false;
        try {
            lowerSegmentId = nextPreparedSegmentId();
            lowerWriterTx = openPreparedWriterTx(lowerSegmentId);
            lowerWriter = openPreparedWriter(lowerSegmentId, lowerWriterTx);
            long lowerCount = 0L;
            long upperCount = 0L;
            K lowerMaxKey = null;
            K upperMaxKey = null;
            while (iterator.hasNext()) {
                final Entry<K, V> entry = iterator.next();
                if (upperWriter == null
                        && lowerCount < validatedTargetLowerCount) {
                    lowerMaxKey = entry.getKey();
                    lowerCount++;
                    writeEntry(lowerWriter, entry);
                    continue;
                }
                if (upperWriter == null) {
                    closePreparedWriter(lowerWriter);
                    upperSegmentId = nextPreparedSegmentId();
                    upperWriterTx = openPreparedWriterTx(upperSegmentId);
                    upperWriter = openPreparedWriter(upperSegmentId,
                            upperWriterTx);
                }
                upperMaxKey = entry.getKey();
                upperCount++;
                writeEntry(upperWriter, entry);
            }
            if (!hasEnoughKeysForSplit(lowerCount, upperCount,
                    validatedMinKeysPerChildSegment)) {
                return RouteSplitPreparation.compactParent();
            }
            commitPreparedSegment(lowerWriterTx, lowerWriter);
            commitPreparedSegment(upperWriterTx, upperWriter);
            materializationCompleted = true;
            return RouteSplitPreparation.prepared(new RouteSplitPlan<>(
                    parentSegment.getId(),
                    lowerSegmentId,
                    upperSegmentId,
                    Vldtn.requireNonNull(lowerMaxKey, "lowerMaxKey"),
                    Vldtn.requireNonNull(upperMaxKey, "upperMaxKey")));
        } finally {
            if (materializationCompleted) {
                closePreparedWriter(lowerWriter);
                closePreparedWriter(upperWriter);
            } else {
                discardPreparedSegment(lowerSegmentId, lowerWriter);
                discardPreparedSegment(upperSegmentId, upperWriter);
            }
        }
    }

    private long requireAtLeastOne(final long value,
            final String propertyName) {
        if (value < 1L) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be >= 1 but was %d.", propertyName,
                    value));
        }
        return value;
    }

    private SegmentId nextPreparedSegmentId() {
        final SegmentId segmentId = Vldtn.requireNonNull(
                segmentRegistry.materialization().nextSegmentId(),
                SEGMENT_ID_ARG);
        ensureSegmentDirectory(segmentId);
        return segmentId;
    }

    private WriteTransaction<K, V> openPreparedWriterTx(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return segmentRegistry.materialization().openWriterTx(segmentId);
    }

    private EntryWriter<K, V> openPreparedWriter(final SegmentId segmentId,
            final WriteTransaction<K, V> writerTx) {
        try {
            return Vldtn.requireNonNull(
                    Vldtn.requireNonNull(writerTx, "writerTx").open(),
                    "writer");
        } catch (final RuntimeException ex) {
            deletePreparedSegmentFiles(segmentId);
            throw ex;
        }
    }

    void deletePreparedSegment(final SegmentId segmentId) {
        deletePreparedSegmentFiles(segmentId);
    }

    private boolean hasEnoughKeysForSplit(final long lowerCount,
            final long upperCount, final long minKeysPerChildSegment) {
        return lowerCount >= minKeysPerChildSegment
                && upperCount >= minKeysPerChildSegment;
    }

    private void writeEntry(final EntryWriter<K, V> writer,
            final Entry<K, V> entry) {
        Vldtn.requireNonNull(writer, "writer")
                .write(Vldtn.requireNonNull(entry, "entry"));
    }

    private void commitPreparedSegment(final WriteTransaction<K, V> writerTx,
            final EntryWriter<K, V> writer) {
        Vldtn.requireNonNull(writerTx, "writerTx");
        closePreparedWriter(writer);
        writerTx.commit();
    }

    private void discardPreparedSegment(final SegmentId segmentId,
            final EntryWriter<K, V> writer) {
        if (segmentId == null) {
            return;
        }
        closePreparedWriter(writer);
        deletePreparedSegmentFiles(segmentId);
    }

    private void closePreparedWriter(final EntryWriter<K, V> writer) {
        if (writer != null && !writer.wasClosed()) {
            writer.close();
        }
    }

    private void ensureSegmentDirectory(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        directoryFacade.openSubDirectory(segmentId.getName());
    }

    private void deletePreparedSegmentFiles(final SegmentId segmentId) {
        segmentRegistry.deleteSegment(
                Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG));
    }
}
