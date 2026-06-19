package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.SegmentId;

/**
 * Prepared upper segment state produced while materializing a route split.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class PreparedUpperSegment<K, V> {

    private final SegmentId segmentId;
    private final WriteTransaction<K, V> writerTx;
    private final EntryWriter<K, V> writer;
    private final K lowerMaxKey;
    private final K upperMaxKey;

    PreparedUpperSegment(final SegmentId segmentId,
            final WriteTransaction<K, V> writerTx,
            final EntryWriter<K, V> writer, final K lowerMaxKey,
            final K upperMaxKey) {
        this.segmentId = segmentId;
        this.writerTx = writerTx;
        this.writer = writer;
        this.lowerMaxKey = lowerMaxKey;
        this.upperMaxKey = upperMaxKey;
    }

    SegmentId segmentId() {
        return segmentId;
    }

    WriteTransaction<K, V> writerTx() {
        return writerTx;
    }

    EntryWriter<K, V> writer() {
        return writer;
    }

    K lowerMaxKey() {
        return lowerMaxKey;
    }

    K upperMaxKey() {
        return upperMaxKey;
    }
}
