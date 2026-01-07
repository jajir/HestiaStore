package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentWriterTxFactoryTest {

    @Test
    void opensWriterTransaction() {
        final WriteTransaction<String, String> tx = mock(WriteTransaction.class);
        final SegmentWriterTxFactory<String, String> factory = segmentId -> tx;

        assertSame(tx, factory.openWriterTx(SegmentId.of(1)));
    }
}
