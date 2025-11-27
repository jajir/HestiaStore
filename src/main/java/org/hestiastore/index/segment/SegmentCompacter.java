package org.hestiastore.index.segment;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class is responsible for compacting segment. It also verify if segment should
 * be compacted.
 */
public final class SegmentCompacter<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VersionController versionController;
    private final SegmentCompactionPolicyWithManager compactionPolicy;

    public SegmentCompacter(
            final VersionController versionController,
            final SegmentCompactionPolicyWithManager compactionPolicy) {
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.compactionPolicy = Vldtn.requireNonNull(compactionPolicy,
                "compactionPolicy");
    }

    /**
     * Optionally compact segment. Method check if segment should be compacted
     * and if should be than it compact it.
     * 
     * @return return <code>true</code> when segment was compacted.
     */
    public void optionallyCompact(final SegmentImpl<K, V> segment) {
        if (compactionPolicy.shouldCompact()) {
            forceCompact(segment);
        }
    }

    public void forceCompact(final SegmentImpl<K, V> segment) {
        final SegmentFiles<K, V> segmentFiles = segment.getSegmentFiles();
        logger.debug("Start of compacting '{}'", segmentFiles.getId());
        segment.resetSegmentIndexSearcher();
        versionController.changeVersion();
        segment.executeFullWriteTx(writer -> {
            try (EntryIterator<K, V> iterator = segment.openIterator()) {
                Entry<K, V> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    writer.write(entry);
                }
            }
        });
        logger.debug("End of compacting '{}'", segmentFiles.getId());
    }

}
