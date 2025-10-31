package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.ByteSequence;
import org.junit.jupiter.api.Test;

public class DataBlockHeaderTest {

    private static final long CRC = 0x5AD3F91C7E28A46BL;

    @Test
    void test_storeAndRetrieveHeader() {
        DataBlockHeader header = DataBlockHeader
                .of(DataBlockHeader.MAGIC_NUMBER, CRC);
        assertEquals(DataBlockHeader.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(CRC, header.getCrc());

        ByteSequence headerBytes = header.getBytes();
        assertNotNull(headerBytes);
        assertEquals(16, headerBytes.length());
        final String headerString = new String(headerBytes.toByteArray());
        assertTrue(headerString.startsWith("nicholas"));

        DataBlockHeader header2 = DataBlockHeader.of(headerBytes);

        assertEquals(DataBlockHeader.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(CRC, header2.getCrc());
    }

}
