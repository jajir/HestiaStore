package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataBlockByteReaderTest {

    private static final DataBlockPayload dataBlockPayload1 = DataBlockPayload
            .ofSequence(ByteSequences.viewOf(TestData.BYTE_ARRAY_1024, 0, 64));
    private static final DataBlockPayload dataBlockPayload2 = DataBlockPayload
            .ofSequence(ByteSequences.viewOf(TestData.BYTE_ARRAY_1024, 64, 128));
    private static final DataBlockPayload dataBlockPayload3 = DataBlockPayload
            .ofSequence(ByteSequences.viewOf(TestData.BYTE_ARRAY_1024, 128,
                    192));

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
        when(dataBlockReader.readPayloadSequence())//
                .thenReturn(dataBlockPayload1.getBytesSequence())//
                .thenReturn(null);
        reader = new DataBlockByteReaderImpl(dataBlockReader, dataBlockSize, 2);

        final ByteSequence bytes1 = reader.readExactlySequence(32);
        final ByteSequence expectedBytes1 = TestData.BYTES_1024.slice(32,
                32 + 32);
        assertEquals(true, ByteSequences.contentEquals(expectedBytes1, bytes1));
        assertNull(reader.readExactlySequence(32));
    }

    @Test
    void test_read_64_one() {
        when(dataBlockReader.readPayloadSequence())//
                .thenReturn(dataBlockPayload1.getBytesSequence())//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactlySequence(64);

        assertEquals(true,
                ByteSequences.contentEquals(TestData.BYTES_1024.slice(0, 64),
                        bytes1));
        assertNull(reader.readExactlySequence(64));
    }

    @Test
    void test_readSequence_64_one() {
        when(dataBlockReader.readPayloadSequence())//
                .thenReturn(dataBlockPayload1.getBytesSequence())//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactlySequence(64);

        assertEquals(true,
                ByteSequences.contentEquals(TestData.BYTES_1024.slice(0, 64),
                        bytes1));
        assertNull(reader.readExactlySequence(64));
    }

    @Test
    void test_read_64_two() {
        when(dataBlockReader.readPayloadSequence())//
                .thenReturn(dataBlockPayload1.getBytesSequence())//
                .thenReturn(dataBlockPayload2.getBytesSequence())//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactlySequence(64);
        assertEquals(true,
                ByteSequences.contentEquals(TestData.BYTES_1024.slice(0, 64),
                        bytes1));

        final ByteSequence bytes2 = reader.readExactlySequence(64);
        assertEquals(true,
                ByteSequences.contentEquals(TestData.BYTES_1024.slice(64, 128),
                        bytes2));

        assertNull(reader.readExactlySequence(64));
        assertNull(reader.readExactlySequence(128));
    }

    @Test
    void test_read_96_one() {
        when(dataBlockReader.readPayloadSequence())//
                .thenReturn(dataBlockPayload1.getBytesSequence())//
                .thenReturn(dataBlockPayload2.getBytesSequence())//
                .thenReturn(null);

        final ByteSequence bytes1 = reader.readExactlySequence(96);
        assertEquals(true,
                ByteSequences.contentEquals(TestData.BYTES_1024.slice(0, 96),
                        bytes1));

        assertNull(reader.readExactlySequence(64));
        assertNull(reader.readExactlySequence(128));
    }

    @Test
    void test_readSequence_160_three_blocks() {
        when(dataBlockReader.readPayloadSequence())//
                .thenReturn(dataBlockPayload1.getBytesSequence())//
                .thenReturn(dataBlockPayload2.getBytesSequence())//
                .thenReturn(dataBlockPayload3.getBytesSequence())//
                .thenReturn(null);

        final ByteSequence bytes = reader.readExactlySequence(160);

        assertEquals(true,
                ByteSequences.contentEquals(TestData.BYTES_1024.slice(0, 160),
                        bytes));
        assertEquals(true, ByteSequences.contentEquals(
                TestData.BYTES_1024.slice(160, 192),
                reader.readExactlySequence(32)));
        assertNull(reader.readExactlySequence(32));
    }

    @Test
    void test_read_empty() {
        assertNull(reader.readExactlySequence(64));
        assertNull(reader.readExactlySequence(128));
    }

}
