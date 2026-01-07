package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.WriteTransaction.WriterFunction;

final class SegmentMaintenancePath<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentResources<K, V> segmentResources;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentCache<K, V> segmentCache;

    SegmentMaintenancePath(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentResources,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentCache<K, V> segmentCache) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentResources = Vldtn.requireNonNull(segmentResources,
                "segmentResources");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
    }

    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        openFullWriteTx().execute(writeFunction);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return new SegmentFullWriterTx<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInChunk(), segmentResources,
                deltaCacheController, segmentCache);
    }

    void flushFrozenWriteCacheToDeltaFile(final List<Entry<K, V>> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        try (EntryWriter<K, V> writer = openDeltaCacheWriter()) {
            entries.forEach(writer::write);
        }
    }

    private EntryWriter<K, V> openDeltaCacheWriter() {
        return new SegmentDeltaCacheCompactingWriter<>(deltaCacheController);
    }
}
