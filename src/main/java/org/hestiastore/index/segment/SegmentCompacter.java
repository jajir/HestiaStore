package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class is responsible for compacting segment. It also verify if segment should
 * be compacted.
 */
final class SegmentCompacter<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VersionController versionController;

    public SegmentCompacter(final VersionController versionController) {
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
    }

    /**
     * Captures a stable snapshot for compaction while the segment is frozen.
     *
     * @param segment segment core
     * @return sorted snapshot entries
     */
    public List<Entry<K, V>> prepareCompaction(final SegmentCore<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        logger.debug("Start of compacting '{}'", segment.getId());
        segment.resetSegmentIndexSearcher();
        segment.freezeWriteCacheForFlush();
        return segment.snapshotCacheEntries();
    }

    /**
     * Performs compaction using a previously captured snapshot.
     *
     * @param segment segment core
     * @param snapshotEntries snapshot entries captured in FREEZE
     */
    public void compact(final SegmentCore<K, V> segment,
            final List<Entry<K, V>> snapshotEntries) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(snapshotEntries, "snapshotEntries");
        final SegmentFullWriterTx<K, V> writerTx = segment.openFullWriteTx();
        writeCompaction(segment, snapshotEntries, writerTx);
        publishCompaction(segment, writerTx);
    }

    void writeCompaction(final SegmentCore<K, V> segment,
            final List<Entry<K, V>> snapshotEntries,
            final SegmentFullWriterTx<K, V> writerTx) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(snapshotEntries, "snapshotEntries");
        Vldtn.requireNonNull(writerTx, "writerTx");
        try (EntryWriter<K, V> writer = writerTx.open();
                EntryIterator<K, V> iterator = segment
                        .openIteratorFromSnapshot(snapshotEntries)) {
            while (iterator.hasNext()) {
                writer.write(iterator.next());
            }
        }
    }

    void publishCompaction(final SegmentCore<K, V> segment,
            final SegmentFullWriterTx<K, V> writerTx) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(writerTx, "writerTx");
        writerTx.commit();
        versionController.changeVersion();
        logger.debug("End of compacting '{}'", segment.getId());
    }

    public void forceCompact(final SegmentCore<K, V> segment) {
        final List<Entry<K, V>> snapshotEntries = prepareCompaction(segment);
        compact(segment, snapshotEntries);
    }

}
