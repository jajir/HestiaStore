package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentWriterTxFactoryTest {

    @Mock
    private WriteTransaction<String, String> tx;

    @Test
    void opensWriterTransaction() {
        final SegmentWriterTxFactory<String, String> factory = segmentId -> tx;

        assertSame(tx, factory.openWriterTx(SegmentId.of(1)));
    }
}
