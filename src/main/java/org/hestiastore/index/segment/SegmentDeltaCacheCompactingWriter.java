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
class SegmentDeltaCacheCompactingWriter<K, V>
        extends AbstractCloseableResource implements EntryWriter<K, V> {

    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    /**
     * holds current delta cache writer.
     */
    private SegmentDeltaCacheWriter<K, V> deltaCacheWriter;

    public SegmentDeltaCacheCompactingWriter(
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
    }

    @Override
    protected void doClose() {
        if (deltaCacheWriter != null) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
        }
    }

    @Override
    public void write(final Entry<K, V> entry) {
        optionallyOpenDeltaCacheWriter();
        deltaCacheWriter.write(entry);
    }

    private void optionallyOpenDeltaCacheWriter() {
        if (deltaCacheWriter == null) {
            deltaCacheWriter = deltaCacheController.openWriter();
        }
    }
}
