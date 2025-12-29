package org.hestiastore.index.segment;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;

/**
 * Allows to add data to segment. When searcher is in memory and number of added
 * keys doesn't exceed limit than it could work without invalidating cache and
 * searcher object..
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDeltaCacheCompactingWriter<K, V>
        extends AbstractCloseableResource implements EntryWriter<K, V> {

    static final int MAX_DELTA_FILES_BEFORE_COMPACTION = 100;

    private final SegmentImpl<K, V> segment;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentCompactionPolicyWithManager compactionPolicy;

    /**
     * holds current delta cache writer.
     */
    private SegmentDeltaCacheWriter<K, V> deltaCacheWriter;

    public SegmentDeltaCacheCompactingWriter(final SegmentImpl<K, V> segment,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentCompactionPolicyWithManager compactionPolicy) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.compactionPolicy = Vldtn.requireNonNull(compactionPolicy,
                "compactionPolicy");
    }

    @Override
    protected void doClose() {
        if (deltaCacheWriter != null) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            if (shouldForceCompaction()) {
                segment.requestCompaction();
            } else {
                segment.requestOptionalCompaction();
            }
        }
    }

    @Override
    public void write(final Entry<K, V> entry) {
        optionallyOpenDeltaCacheWriter();
        deltaCacheWriter.write(entry);
        if (compactionPolicy.shouldCompactDuringWriting(
                deltaCacheWriter.getNumberOfKeys())) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            segment.requestCompaction();
        }
    }

    private void optionallyOpenDeltaCacheWriter() {
        if (deltaCacheWriter == null) {
            deltaCacheWriter = deltaCacheController.openWriter();
        }
    }

    private boolean shouldForceCompaction() {
        return segment.getSegmentPropertiesManager().getDeltaFileCount()
                > MAX_DELTA_FILES_BEFORE_COMPACTION;
    }

}
