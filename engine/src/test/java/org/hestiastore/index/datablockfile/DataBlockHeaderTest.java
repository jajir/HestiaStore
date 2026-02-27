package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class DataBlockHeaderTest {

    private static final long CRC = 0x5AD3F91C7E28A46BL;

    @Test
    void test_storeAndRetrieveHeader() {
        DataBlockHeader header = DataBlockHeader
                .of(DataBlockHeader.MAGIC_NUMBER, CRC);
        assertEquals(DataBlockHeader.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(CRC, header.getCrc());

        ByteSequence headerBytes = header.toBytesSequence();
        assertNotNull(headerBytes);
        assertEquals(16, headerBytes.length());
        final String headerString = new String(headerBytes.toByteArrayCopy());
        assertTrue(headerString.startsWith("nicholas"));

        DataBlockHeader header2 = DataBlockHeader.ofSequence(headerBytes);

        assertEquals(DataBlockHeader.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(CRC, header2.getCrc());
    }

    @Test
    void test_ofSequence_from_copy() {
        DataBlockHeader header1 = DataBlockHeader
                .of(DataBlockHeader.MAGIC_NUMBER, CRC);

        DataBlockHeader header2 = DataBlockHeader.ofSequence(ByteSequences
                .copyOf(header1.toBytesSequence().toByteArrayCopy()));

        assertEquals(DataBlockHeader.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(CRC, header2.getCrc());
    }

    @Test
    void test_ofSequence() {
        DataBlockHeader header1 = DataBlockHeader
                .of(DataBlockHeader.MAGIC_NUMBER, CRC);

        DataBlockHeader header2 = DataBlockHeader.ofSequence(
                header1.toBytesSequence());

        assertEquals(DataBlockHeader.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(CRC, header2.getCrc());
    }

}
