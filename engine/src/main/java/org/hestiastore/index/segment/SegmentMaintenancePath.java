package org.hestiastore.index.segment;

import java.util.Iterator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction.WriterFunction;

/**
 * Encapsulates maintenance operations like flush and full rewrite transactions.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentMaintenancePath<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentResources<K, V> segmentResources;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    SegmentMaintenancePath(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentResources,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentResources = Vldtn.requireNonNull(segmentResources,
                "segmentResources");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
    }

    /**
     * Executes a full write transaction over the segment files.
     *
     * @param writeFunction writer logic to execute
     */
    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        openFullWriteTx().execute(writeFunction);
    }

    /**
     * Opens a new full write transaction.
     *
     * @return writable transaction
     */
    SegmentFullWriterTx<K, V> openFullWriteTx() {
        return new SegmentFullWriterTx<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInChunk(), segmentResources,
                deltaCacheController);
    }

    /**
     * Flushes a frozen write-cache snapshot into the delta cache file.
     *
     * @param entries iterator over frozen write-cache entries
     */
    void flushFrozenWriteCacheToDeltaFile(
            final Iterator<Entry<K, V>> entries) {
        if (entries == null || !entries.hasNext()) {
            return;
        }
        try (EntryWriter<K, V> writer = openDeltaCacheWriter()) {
            while (entries.hasNext()) {
                writer.write(entries.next());
            }
        }
    }

    /**
     * Opens a writer that appends to the delta cache.
     *
     * @return delta cache writer
     */
    private EntryWriter<K, V> openDeltaCacheWriter() {
        return new SegmentDeltaCacheCompactingWriter<>(deltaCacheController);
    }

    SegmentConf getSegmentConf() {
        return segmentConf;
    }

    SegmentDeltaCacheController<K, V> getDeltaCacheController() {
        return deltaCacheController;
    }
}
