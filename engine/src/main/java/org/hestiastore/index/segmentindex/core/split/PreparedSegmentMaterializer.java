package org.hestiastore.index.segmentindex.core.split;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.IndexException;
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
    private final SegmentRegistry.Materialization<K, V> materialization;

    /**
     * Creates a materialization service backed by the provided collaborators.
     *
     * @param directoryFacade root directory for segment storage
     * @param materialization registry materialization view used to allocate ids
     *                        and open synchronous segment writers
     */
    PreparedSegmentMaterializer(
            final Directory directoryFacade,
            final SegmentRegistry.Materialization<K, V> materialization) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.materialization = Vldtn.requireNonNull(materialization,
                "materialization");
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
                materialization.nextSegmentId(), SEGMENT_ID_ARG);
        ensureSegmentDirectory(segmentId);
        return segmentId;
    }

    private WriteTransaction<K, V> openPreparedWriterTx(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return materialization.openWriterTx(segmentId);
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
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final PreparedSegmentCleanupErrors failures =
                new PreparedSegmentCleanupErrors();
        deleteDirectory(segmentId.getName(), failures);
        failures.throwIfAny();
    }

    private void deleteDirectory(final String directoryName,
            final PreparedSegmentCleanupErrors failures) {
        if (!exists(directoryFacade, directoryName)) {
            return;
        }
        final Directory directory;
        try {
            directory = directoryFacade.openSubDirectory(directoryName);
        } catch (final RuntimeException ex) {
            failures.add(ex);
            ensureEntryDeleted(directoryFacade, directoryName, failures);
            return;
        }
        clearDirectory(directory, failures);
        try {
            directoryFacade.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            failures.add(ex);
        }
        ensureEntryDeleted(directoryFacade, directoryName, failures);
    }

    private void clearDirectory(final Directory directory,
            final PreparedSegmentCleanupErrors failures) {
        try (Stream<String> entries = directory.getFileNames()) {
            final java.util.Iterator<String> iterator = entries.iterator();
            while (iterator.hasNext()) {
                final String entry = iterator.next();
                deleteDirectoryEntry(directory, entry, failures);
            }
        } catch (final RuntimeException ex) {
            failures.add(ex);
        }
    }

    private void deleteDirectoryEntry(final Directory directory,
            final String entryName,
            final PreparedSegmentCleanupErrors failures) {
        try {
            if (directory.deleteFile(entryName)) {
                return;
            }
        } catch (final RuntimeException ex) {
            failures.add(ex);
        }
        if (!exists(directory, entryName)) {
            return;
        }
        deleteSubDirectory(directory, entryName, failures);
    }

    private boolean exists(final Directory directory, final String fileName) {
        try {
            return directory.isFileExists(fileName);
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private void deleteSubDirectory(final Directory directory,
            final String directoryName,
            final PreparedSegmentCleanupErrors failures) {
        final Directory subDirectory;
        try {
            subDirectory = directory.openSubDirectory(directoryName);
        } catch (final RuntimeException ex) {
            failures.add(ex);
            return;
        }
        clearDirectory(subDirectory, failures);
        try {
            directory.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            failures.add(ex);
        }
        ensureEntryDeleted(directory, directoryName, failures);
    }

    private void ensureEntryDeleted(final Directory directory,
            final String entryName,
            final PreparedSegmentCleanupErrors failures) {
        if (!exists(directory, entryName)) {
            return;
        }
        failures.add(new IndexException(String.format(
                "Prepared segment entry '%s' was not fully deleted.",
                entryName)));
    }
}
