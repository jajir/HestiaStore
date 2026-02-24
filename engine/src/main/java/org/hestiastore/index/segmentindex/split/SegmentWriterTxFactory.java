package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.SegmentId;

@FunctionalInterface
public interface SegmentWriterTxFactory<K, V> {
    WriteTransaction<K, V> openWriterTx(SegmentId segmentId);
}
