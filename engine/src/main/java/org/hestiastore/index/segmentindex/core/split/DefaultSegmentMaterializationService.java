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
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Default implementation of offline segment materialization for route splits.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DefaultSegmentMaterializationService<K, V> {

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
    DefaultSegmentMaterializationService(
            final Directory directoryFacade,
            final SegmentRegistry.Materialization<K, V> materialization) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.materialization = Vldtn.requireNonNull(materialization,
                "materialization");
    }

    RouteSplitPlan<K> materializeRouteSplit(
            final Segment<K, V> parentSegment,
            final long targetLowerCount,
            final EntryIterator<K, V> iterator) {
        Vldtn.requireNonNull(parentSegment, "parentSegment");
        Vldtn.requireNonNull(iterator, "iterator");
        final long validatedTargetLowerCount = requireTargetLowerCount(
                targetLowerCount);
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
            final MaterializedUpperSegment<K, V> upperSegment = writeSplitEntries(
                    iterator, validatedTargetLowerCount, lowerWriter);
            upperSegmentId = upperSegment.segmentId();
            upperWriterTx = upperSegment.writerTx();
            upperWriter = upperSegment.writer();
            commitPreparedSegment(lowerWriterTx, lowerWriter);
            commitPreparedSegment(upperWriterTx, upperWriter);
            materializationCompleted = true;
            final K lowerMaxKey = upperSegment.lowerMaxKey();
            return new RouteSplitPlan<>(
                    parentSegment.getId(),
                    lowerSegmentId,
                    upperSegmentId,
                    lowerMaxKey,
                    RouteSplitPlan.SplitMode.SPLIT);
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

    private long requireTargetLowerCount(final long targetLowerCount) {
        if (targetLowerCount < 1L) {
            throw new IllegalArgumentException(String.format(
                    "Property 'targetLowerCount' must be >= 1 but was %d.",
                    targetLowerCount));
        }
        return targetLowerCount;
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

    private MaterializedUpperSegment<K, V> writeSplitEntries(
            final EntryIterator<K, V> iterator,
            final long targetLowerCount,
            final EntryWriter<K, V> lowerWriter) {
        SegmentId upperSegmentId = null;
        WriteTransaction<K, V> upperWriterTx = null;
        EntryWriter<K, V> upperWriter = null;
        K lowerMaxKey = null;
        long lowerCount = 0L;
        while (iterator.hasNext()) {
            final Entry<K, V> entry = iterator.next();
            if (lowerCount < targetLowerCount) {
                lowerMaxKey = entry.getKey();
                lowerCount++;
                writeEntry(lowerWriter, entry);
                continue;
            }
            if (upperWriter == null) {
                upperSegmentId = nextPreparedSegmentId();
                upperWriterTx = openPreparedWriterTx(upperSegmentId);
                upperWriter = openPreparedWriter(upperSegmentId,
                        upperWriterTx);
            }
            writeEntry(upperWriter, entry);
        }
        return new MaterializedUpperSegment<>(
                Vldtn.requireNonNull(upperSegmentId, "upperSegmentId"),
                Vldtn.requireNonNull(upperWriterTx, "upperWriterTx"),
                Vldtn.requireNonNull(upperWriter, "upperWriter"),
                Vldtn.requireNonNull(lowerMaxKey, "lowerMaxKey"));
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
        final RuntimeException cleanupFailure = deleteDirectory(
                segmentId.getName());
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private RuntimeException deleteDirectory(final String directoryName) {
        if (!exists(directoryFacade, directoryName)) {
            return null;
        }
        RuntimeException cleanupFailure = null;
        final Directory directory;
        try {
            directory = directoryFacade.openSubDirectory(directoryName);
        } catch (final RuntimeException ex) {
            cleanupFailure = mergeFailure(cleanupFailure, ex);
            return ensureEntryDeleted(directoryFacade, directoryName,
                    cleanupFailure);
        }
        cleanupFailure = clearDirectory(directory, cleanupFailure);
        try {
            directoryFacade.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            cleanupFailure = mergeFailure(cleanupFailure, ex);
        }
        return ensureEntryDeleted(directoryFacade, directoryName,
                cleanupFailure);
    }

    private RuntimeException clearDirectory(final Directory directory,
            final RuntimeException cleanupFailure) {
        RuntimeException currentFailure = cleanupFailure;
        try (Stream<String> entries = directory.getFileNames()) {
            final java.util.Iterator<String> iterator = entries.iterator();
            while (iterator.hasNext()) {
                final String entry = iterator.next();
                currentFailure = deleteDirectoryEntry(directory, entry,
                        currentFailure);
            }
        } catch (final RuntimeException ex) {
            currentFailure = mergeFailure(currentFailure, ex);
        }
        return currentFailure;
    }

    private RuntimeException deleteDirectoryEntry(final Directory directory,
            final String entryName, final RuntimeException cleanupFailure) {
        RuntimeException currentFailure = cleanupFailure;
        try {
            if (directory.deleteFile(entryName)) {
                return currentFailure;
            }
        } catch (final RuntimeException ex) {
            currentFailure = mergeFailure(currentFailure, ex);
        }
        if (!exists(directory, entryName)) {
            return currentFailure;
        }
        return deleteSubDirectory(directory, entryName, currentFailure);
    }

    private boolean exists(final Directory directory, final String fileName) {
        try {
            return directory.isFileExists(fileName);
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private RuntimeException deleteSubDirectory(final Directory directory,
            final String directoryName, final RuntimeException cleanupFailure) {
        RuntimeException currentFailure = cleanupFailure;
        final Directory subDirectory;
        try {
            subDirectory = directory.openSubDirectory(directoryName);
        } catch (final RuntimeException ex) {
            return mergeFailure(currentFailure, ex);
        }
        currentFailure = clearDirectory(subDirectory, currentFailure);
        try {
            directory.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            currentFailure = mergeFailure(currentFailure, ex);
        }
        return ensureEntryDeleted(directory, directoryName, currentFailure);
    }

    private RuntimeException ensureEntryDeleted(final Directory directory,
            final String entryName, final RuntimeException cleanupFailure) {
        RuntimeException currentFailure = cleanupFailure;
        if (!exists(directory, entryName)) {
            return currentFailure;
        }
        return mergeFailure(currentFailure, new IndexException(String.format(
                "Prepared segment entry '%s' was not fully deleted.",
                entryName)));
    }

    private RuntimeException mergeFailure(
            final RuntimeException currentFailure,
            final RuntimeException nextFailure) {
        if (currentFailure == null) {
            return nextFailure;
        }
        currentFailure.addSuppressed(nextFailure);
        return currentFailure;
    }

    private static final class MaterializedUpperSegment<K, V> {

        private final SegmentId segmentId;
        private final WriteTransaction<K, V> writerTx;
        private final EntryWriter<K, V> writer;
        private final K lowerMaxKey;

        private MaterializedUpperSegment(final SegmentId segmentId,
                final WriteTransaction<K, V> writerTx,
                final EntryWriter<K, V> writer, final K lowerMaxKey) {
            this.segmentId = segmentId;
            this.writerTx = writerTx;
            this.writer = writer;
            this.lowerMaxKey = lowerMaxKey;
        }

        private SegmentId segmentId() {
            return segmentId;
        }

        private WriteTransaction<K, V> writerTx() {
            return writerTx;
        }

        private EntryWriter<K, V> writer() {
            return writer;
        }

        private K lowerMaxKey() {
            return lowerMaxKey;
        }
    }
}
