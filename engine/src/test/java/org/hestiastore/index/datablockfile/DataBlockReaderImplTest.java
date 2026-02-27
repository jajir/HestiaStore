package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataBlockReaderImplTest {

    private static final DataBlockSize BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    private static final DataBlockPosition BLOCK_POSITION = DataBlockPosition
            .of(2048);

    @Mock
    private FileReaderSeekable fileReader;

    private DataBlockReaderImpl reader;

    @BeforeEach
    void beforeEach() {
        reader = new DataBlockReaderImpl(fileReader, BLOCK_POSITION,
                BLOCK_SIZE, true);
    }

    @AfterEach
    void afterEach() {
        reader.close();
    }

    @Test
    void test_read() {
        final DataBlockHeader header = DataBlockHeader.of(
                DataBlockHeader.MAGIC_NUMBER,
                TestData.PAYLOAD_1008.calculateCrc());

        byte[] bufferBytes = buildBlockBytes(header,
                TestData.PAYLOAD_1008.getBytesSequence());

        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);
            assertEquals(0, offset);
            assertEquals(1024, length);
            System.arraycopy(bufferBytes, 0, buffer, 0, 1024);
            return 1024;
                });
        DataBlock ret1 = reader.read();
        assertNotNull(ret1);
        assertArrayEquals(bufferBytes, ret1.getBytesSequence().toByteArrayCopy());
        assertEquals(2048, ret1.getPosition().getValue());
    }

    @Test
    void test_readPayloadSequence() {
        final DataBlockHeader header = DataBlockHeader.of(
                DataBlockHeader.MAGIC_NUMBER,
                TestData.PAYLOAD_1008.calculateCrc());

        byte[] bufferBytes = buildBlockBytes(header,
                TestData.PAYLOAD_1008.getBytesSequence());

        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);
            assertEquals(0, offset);
            assertEquals(1024, length);
            System.arraycopy(bufferBytes, 0, buffer, 0, 1024);
            return 1024;
                });
        final ByteSequence payload = reader.readPayloadSequence();
        assertNotNull(payload);
        assertArrayEquals(TestData.PAYLOAD_1008.getBytesSequence().toByteArrayCopy(),
                payload.toByteArrayCopy());
    }

    @Test
    void test_read_invalid_crc() {
        byte[] bufferBytes = new byte[1024];
        System.arraycopy(TestData.BYTE_ARRAY_1024, 0, bufferBytes, 0, 1024);
        DataBlockHeader header = DataBlockHeader
                .of(DataBlockHeader.MAGIC_NUMBER, 2131L);
        ByteSequences.copy(header.toBytesSequence(), 0, bufferBytes, 0, 16);

        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);
            assertEquals(0, offset);
            assertEquals(1024, length);
            System.arraycopy(bufferBytes, 0, buffer, 0, 1024);
            return 1024;
                });

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> reader.read());
        assertEquals("CRC mismatch in data block header", e.getMessage());
    }

    @Test
    void test_readPayloadSequence_invalid_crc() {
        byte[] bufferBytes = new byte[1024];
        System.arraycopy(TestData.BYTE_ARRAY_1024, 0, bufferBytes, 0, 1024);
        DataBlockHeader header = DataBlockHeader
                .of(DataBlockHeader.MAGIC_NUMBER, 2131L);
        ByteSequences.copy(header.toBytesSequence(), 0, bufferBytes, 0, 16);

        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);
            assertEquals(0, offset);
            assertEquals(1024, length);
            System.arraycopy(bufferBytes, 0, buffer, 0, 1024);
            return 1024;
                });

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> reader.readPayloadSequence());
        assertEquals("CRC mismatch in data block header", e.getMessage());
    }

    @Test
    void test_read_invalidBlockSize_was_readed() {
        final int[] calls = new int[] { 0 };
        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);
            if (calls[0] == 0) {
                assertEquals(0, offset);
                assertEquals(1024, length);
                System.arraycopy(TestData.BYTE_ARRAY_1024, 0, buffer, 0, 45);
                calls[0]++;
                return 45;
            }
            return -1;
                });
        final Exception e = assertThrows(IndexException.class,
                () -> reader.read());

        assertEquals("Unable to read full block", e.getMessage());
    }

    @Test
    void test_propagateException() {
        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenThrow(new IndexException("Test Exception"));

        assertThrows(IndexException.class, () -> reader.read());
    }

    @Test
    void test_read_end_of_file() {
        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenReturn(-1, -1);
        assertNull(reader.read());
        assertNull(reader.read());
    }

    @Test
    void test_readPayloadSequence_end_of_file() {
        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenReturn(-1, -1);
        assertNull(reader.readPayloadSequence());
        assertNull(reader.readPayloadSequence());
    }

    @Test
    void test_readPayloadSequence_allows_partial_reads_until_full_block() {
        final DataBlockHeader header = DataBlockHeader.of(
                DataBlockHeader.MAGIC_NUMBER,
                TestData.PAYLOAD_1008.calculateCrc());
        final byte[] blockBytes = buildBlockBytes(header,
                TestData.PAYLOAD_1008.getBytesSequence());
        final int[] sourcePosition = new int[] { 0 };

        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
                    final byte[] buffer = invocation.getArgument(0);
                    final int offset = invocation.getArgument(1);
                    final int length = invocation.getArgument(2);
                    if (sourcePosition[0] >= blockBytes.length) {
                        return -1;
                    }
                    final int read = Math.min(200,
                            Math.min(length,
                                    blockBytes.length - sourcePosition[0]));
                    System.arraycopy(blockBytes, sourcePosition[0], buffer,
                            offset, read);
                    sourcePosition[0] += read;
                    return read;
                });

        final ByteSequence payload = reader.readPayloadSequence();
        assertNotNull(payload);
        assertArrayEquals(TestData.PAYLOAD_1008.getBytesSequence().toByteArrayCopy(),
                payload.toByteArrayCopy());
    }

    private static byte[] buildBlockBytes(final DataBlockHeader header,
            final ByteSequence payload) {
        final byte[] blockBytes = new byte[DataBlockHeader.HEADER_SIZE
                + payload.length()];
        ByteSequences.copy(header.toBytesSequence(), 0, blockBytes, 0,
                DataBlockHeader.HEADER_SIZE);
        ByteSequences.copy(payload, 0, blockBytes, DataBlockHeader.HEADER_SIZE,
                payload.length());
        return blockBytes;
    }

}
