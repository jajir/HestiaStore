package org.hestiastore.index.segmentindex.core.routing;

import java.util.Comparator;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
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

    public RouteSplitPlan<K> materializeRouteSplit(
            final Segment<K, V> parentSegment,
            final K lowerMinKey,
            final K lowerMaxKey,
            final Comparator<K> keyComparator,
            final EntryIterator<K, V> iterator) {
        Vldtn.requireNonNull(parentSegment, "parentSegment");
        Vldtn.requireNonNull(lowerMinKey, "lowerMinKey");
        Vldtn.requireNonNull(lowerMaxKey, "lowerMaxKey");
        Vldtn.requireNonNull(keyComparator, "keyComparator");
        Vldtn.requireNonNull(iterator, "iterator");
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
                    iterator, lowerMaxKey, keyComparator, lowerWriter);
            upperSegmentId = upperSegment.segmentId();
            upperWriterTx = upperSegment.writerTx();
            upperWriter = upperSegment.writer();
            commitPreparedSegment(lowerWriterTx, lowerWriter);
            commitPreparedSegment(upperWriterTx, upperWriter);
            materializationCompleted = true;
            return new RouteSplitPlan<>(
                    parentSegment.getId(),
                    lowerSegmentId,
                    upperSegmentId,
                    lowerMinKey,
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

    private SegmentId nextPreparedSegmentId() {
        final SegmentId segmentId = Vldtn.requireNonNull(
                materialization.nextSegmentId(), "segmentId");
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

    public void deletePreparedSegment(final SegmentId segmentId) {
        deletePreparedSegmentFiles(segmentId);
    }

    private MaterializedUpperSegment<K, V> writeSplitEntries(
            final EntryIterator<K, V> iterator,
            final K lowerMaxKey,
            final Comparator<K> keyComparator,
            final EntryWriter<K, V> lowerWriter) {
        SegmentId upperSegmentId = null;
        WriteTransaction<K, V> upperWriterTx = null;
        EntryWriter<K, V> upperWriter = null;
        while (iterator.hasNext()) {
            final Entry<K, V> entry = iterator.next();
            if (isLowerKey(entry.getKey(), lowerMaxKey, keyComparator)) {
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
                Vldtn.requireNonNull(upperWriter, "upperWriter"));
    }

    private boolean isLowerKey(final K key, final K lowerMaxKey,
            final Comparator<K> keyComparator) {
        return keyComparator.compare(key, lowerMaxKey) <= 0;
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
        Vldtn.requireNonNull(segmentId, "segmentId");
        directoryFacade.openSubDirectory(segmentId.getName());
    }

    private void deletePreparedSegmentFiles(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        deleteDirectory(segmentId.getName());
    }

    private void deleteDirectory(final String directoryName) {
        if (!directoryFacade.isFileExists(directoryName)) {
            return;
        }
        final Directory directory = directoryFacade
                .openSubDirectory(directoryName);
        clearDirectory(directory);
        try {
            directoryFacade.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            return;
        }
    }

    private void clearDirectory(final Directory directory) {
        try (Stream<String> entries = directory.getFileNames()) {
            entries.forEach(entry -> {
                if (tryDeleteFile(directory, entry)
                        || !exists(directory, entry)) {
                    return;
                }
                deleteSubDirectory(directory, entry);
            });
        }
    }

    private boolean tryDeleteFile(final Directory directory,
            final String fileName) {
        try {
            return directory.deleteFile(fileName);
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private boolean exists(final Directory directory, final String fileName) {
        try {
            return directory.isFileExists(fileName);
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private void deleteSubDirectory(final Directory directory,
            final String directoryName) {
        try {
            final Directory subDirectory = directory
                    .openSubDirectory(directoryName);
            clearDirectory(subDirectory);
            directory.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            return;
        }
    }

    private static final class MaterializedUpperSegment<K, V> {

        private final SegmentId segmentId;
        private final WriteTransaction<K, V> writerTx;
        private final EntryWriter<K, V> writer;

        private MaterializedUpperSegment(final SegmentId segmentId,
                final WriteTransaction<K, V> writerTx,
                final EntryWriter<K, V> writer) {
            this.segmentId = segmentId;
            this.writerTx = writerTx;
            this.writer = writer;
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
    }
}
