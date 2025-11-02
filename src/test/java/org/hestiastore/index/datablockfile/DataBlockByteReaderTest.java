package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
import org.hestiastore.index.TestData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DataBlockByteReaderTest {

    private static final DataBlockPayload dataBlockPayload1 = DataBlockPayload
            .of(TestData.BYTES_1024.slice(0, 64));
    private static final DataBlockPayload dataBlockPayload2 = DataBlockPayload
            .of(TestData.BYTES_1024.slice(64, 128));

    private static final DataBlockHeader dataBlockHeader1 = DataBlockHeader
            .of(DataBlockHeader.MAGIC_NUMBER, dataBlockPayload1.calculateCrc());
    private static final DataBlockHeader dataBlockHeader2 = DataBlockHeader
            .of(DataBlockHeader.MAGIC_NUMBER, dataBlockPayload2.calculateCrc());

    private static final DataBlock dataBlock1 = DataBlock.of(
            ConcatenatedByteSequence.of(dataBlockHeader1.getBytes(),
                    dataBlockPayload1.getBytes()),
            DataBlockPosition.of(0));
    private static final DataBlock dataBlock2 = DataBlock.of(
            ConcatenatedByteSequence.of(dataBlockHeader2.getBytes(),
                    dataBlockPayload2.getBytes()),
            DataBlockPosition.of(128));

    @Mock
    private DataBlockSize dataBlockSize;

    @Mock
    private DataBlockReader dataBlockReader;

    private DataBlockByteReader reader;

    @BeforeEach
    void beforeEach() {
        when(dataBlockSize.getPayloadSize())//
                .thenReturn(64, 64, 64);
        reader = new DataBlockByteReaderImpl(dataBlockReader, dataBlockSize, 0);
    }

    @AfterEach
    void afterEach() {
        reader.close();
        reader = null;
    }

    @Test
    void test_constructor_initial_cell_in_out_of_range_4() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DataBlockByteReaderImpl(dataBlockReader,
                        dataBlockSize, 4));
        assertEquals(
                "Initial cell index '4' is out of range. Max allowed is '3'",
                e.getMessage());
    }

    @Test
    void test_read_from_cell_2() {
        when(dataBlockReader.read())//
                .thenReturn(dataBlock1)//
                .thenReturn(null);
        reader = new DataBlockByteReaderImpl(dataBlockReader, dataBlockSize, 2);

        final ByteSequence bytes1 = reader.readExactly(32);
        final ByteSequence expectedBytes1 = TestData.BYTES_1024.slice(32,
                32 + 32);
        assertArrayEquals(expectedBytes1.toByteArray(), bytes1.toByteArray());
        assertNull(reader.readExactly(32));
    }

    @Test
    void test_read_64_one() {
        when(dataBlockReader.read())//
                .thenReturn(dataBlock1)//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactly(64);

        assertArrayEquals(TestData.BYTES_1024.slice(0, 64).toByteArray(),
                bytes1.toByteArray());
        assertNull(reader.readExactly(64));
    }

    @Test
    void test_read_64_two() {
        when(dataBlockReader.read())//
                .thenReturn(dataBlock1)//
                .thenReturn(dataBlock2)//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactly(64);
        assertArrayEquals(TestData.BYTES_1024.slice(0, 64).toByteArray(),
                bytes1.toByteArray());

        final ByteSequence bytes2 = reader.readExactly(64);
        assertArrayEquals(TestData.BYTES_1024.slice(64, 128).toByteArray(),
                bytes2.toByteArray());

        assertNull(reader.readExactly(64));
        assertNull(reader.readExactly(128));
    }

    @Test
    void test_read_96_one() {
        when(dataBlockReader.read())//
                .thenReturn(dataBlock1)//
                .thenReturn(dataBlock2)//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactly(96);
        assertArrayEquals(TestData.BYTES_1024.slice(0, 96).toByteArray(),
                bytes1.toByteArray());

        assertNull(reader.readExactly(64));
        assertNull(reader.readExactly(128));
    }

    @Test
    void test_read_empty() {
        when(dataBlockReader.read())//
                .thenReturn(null);

        assertNull(reader.readExactly(64));
        assertNull(reader.readExactly(128));
    }

}
