package org.hestiastore.index.segmentindex;

import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.SegmentId;

@FunctionalInterface
interface SegmentWriterTxFactory<K, V> {
    WriteTransaction<K, V> openWriterTx(SegmentId segmentId);
}
