package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.DataBlock;
import org.hestiastore.index.datablockfile.DataBlockHeader;
import org.hestiastore.index.datablockfile.DataBlockPayload;
import org.hestiastore.index.datablockfile.DataBlockPosition;
import org.hestiastore.index.datablockfile.DataBlockReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkStoreReaderImplTest {

    private static final int DATABLOCK_PAYLOAD_SIZE = 64;

    private static final int VERSION = 682;

    @Mock
    private DataBlockReader dataBlockReader;

    private ChunkStoreReader reader;

    @BeforeEach
    void beforeEach() {
        reader = new ChunkStoreReaderImpl(dataBlockReader,
                DATABLOCK_PAYLOAD_SIZE);
    }

    @AfterEach
    void afterEach() {
        reader.close();
    }

    @Test
    void test_read_small_chunk() {
        final ChunkHeader chunkHeader = ChunkHeader.of(Chunk.MAGIC_NUMBER,
                VERSION, 9, TestData.PAYLOAD_9.calculateCrc());
        final Bytes chunkBytes = chunkHeader.getBytes()
                .add(TestData.BYTES_9.paddedTo(DATABLOCK_PAYLOAD_SIZE));
        DataBlockPayload dataBlockPayload = DataBlockPayload.of(chunkBytes);
        DataBlockHeader dataBlockHeader = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        final DataBlock dataBlock = DataBlock.of(
                dataBlockHeader.toBytes().add(chunkBytes),
                DataBlockPosition.of(0));

        when(dataBlockReader.read()).thenReturn(dataBlock);

        final Chunk chunk = reader.read();
        // Verify that the read method returns the expected chunk.
        assertEquals(TestData.PAYLOAD_9, chunk.getPayload());
        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(9, chunk.getHeader().getPayloadLength());
        assertEquals(TestData.BYTES_9, chunk.getPayload().getBytes());
    }

    @Test
    void test_read_small_chunks() {
        final ChunkHeader chunkHeader1 = ChunkHeader.of(Chunk.MAGIC_NUMBER,
                VERSION, 9, TestData.PAYLOAD_9.calculateCrc());
        final ChunkHeader chunkHeader2 = ChunkHeader.of(Chunk.MAGIC_NUMBER,
                VERSION, 15, TestData.PAYLOAD_15.calculateCrc());

        final Bytes chunkBytes = chunkHeader1.getBytes()//
                .add(TestData.BYTES_9.paddedTo(16))//
                .add(chunkHeader2.getBytes())//
                .add(TestData.BYTES_15.paddedTo(16))//
                .paddedTo(128);

        DataBlockPayload dataBlockPayload1 = DataBlockPayload
                .of(chunkBytes.subBytes(0, 64));
        DataBlockPayload dataBlockPayload2 = DataBlockPayload
                .of(chunkBytes.subBytes(64, 128));

        DataBlockHeader dataBlockHeader1 = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload1.calculateCrc());
        DataBlockHeader dataBlockHeader2 = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload2.calculateCrc());

        final DataBlock dataBlock1 = DataBlock.of(
                dataBlockHeader1.toBytes().add(dataBlockPayload1.getBytes()),
                DataBlockPosition.of(0));
        final DataBlock dataBlock2 = DataBlock.of(
                dataBlockHeader2.toBytes().add(dataBlockPayload2.getBytes()),
                DataBlockPosition.of(128));

        when(dataBlockReader.read()).thenReturn(dataBlock1);

        final Chunk chunk1 = reader.read();
        assertEquals(TestData.PAYLOAD_9, chunk1.getPayload());
        assertEquals(VERSION, chunk1.getHeader().getVersion());
        assertEquals(9, chunk1.getHeader().getPayloadLength());
        assertEquals(TestData.BYTES_9, chunk1.getPayload().getBytes());

        when(dataBlockReader.read()).thenReturn(dataBlock2);

        final Chunk chunk2 = reader.read();
        assertEquals(TestData.PAYLOAD_15, chunk2.getPayload());
        assertEquals(VERSION, chunk2.getHeader().getVersion());
        assertEquals(15, chunk2.getHeader().getPayloadLength());
        assertEquals(TestData.BYTES_15, chunk2.getPayload().getBytes());

    }

    @Test
    void test_read_large_chunk() {
        final ChunkHeader chunkHeader = ChunkHeader.of(Chunk.MAGIC_NUMBER,
                VERSION, 154, TestData.PAYLOAD_154.calculateCrc());
        final Bytes chunkBytes = chunkHeader.getBytes()
                .add(TestData.BYTES_154.paddedTo(160));

        DataBlockPayload dataBlockPayload1 = DataBlockPayload
                .of(chunkBytes.subBytes(0, 64));
        DataBlockPayload dataBlockPayload2 = DataBlockPayload
                .of(chunkBytes.subBytes(64, 128));
        DataBlockPayload dataBlockPayload3 = DataBlockPayload
                .of(chunkBytes.subBytes(128, 192));

        DataBlockHeader dataBlockHeader1 = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload1.calculateCrc());
        DataBlockHeader dataBlockHeader2 = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload2.calculateCrc());
        DataBlockHeader dataBlockHeader3 = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload3.calculateCrc());

        final DataBlock dataBlock1 = DataBlock.of(
                dataBlockHeader1.toBytes().add(dataBlockPayload1.getBytes()),
                DataBlockPosition.of(0));
        final DataBlock dataBlock2 = DataBlock.of(
                dataBlockHeader2.toBytes().add(dataBlockPayload2.getBytes()),
                DataBlockPosition.of(80));
        final DataBlock dataBlock3 = DataBlock.of(
                dataBlockHeader3.toBytes().add(dataBlockPayload3.getBytes()),
                DataBlockPosition.of(160));

        when(dataBlockReader.read()).thenReturn(dataBlock1, dataBlock2,
                dataBlock3);

        final Chunk chunk = reader.read();

        assertEquals(TestData.PAYLOAD_154, chunk.getPayload());
        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(154, chunk.getHeader().getPayloadLength());
        assertEquals(TestData.BYTES_154, chunk.getPayload().getBytes());

    }

    @Test
    void test_read_small_chunk_invalid_magic_number() {
        byte[] bytes = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION, 9,
                TestData.PAYLOAD_9.calculateCrc()).getBytes().getData();
        // manually corrupt the magic number
        bytes[0] = 0;
        bytes[5] = 0;

        final Bytes chunkBytes = Bytes.of(bytes)
                .add(TestData.BYTES_9.paddedTo(DATABLOCK_PAYLOAD_SIZE));
        DataBlockPayload dataBlockPayload = DataBlockPayload.of(chunkBytes);
        DataBlockHeader dataBlockHeader = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        final DataBlock dataBlock = DataBlock.of(
                dataBlockHeader.toBytes().add(chunkBytes),
                DataBlockPosition.of(0));

        when(dataBlockReader.read()).thenReturn(dataBlock);

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> reader.read());

        // Verify that the read method returns the expected chunk.
        assertEquals(
                "Invalid chunk magic number '29384926671434337', expected is '8388065835078349409'",
                e.getMessage());
    }

    @Test
    void test_read_small_chunk_invalid_CRC() {
        ChunkHeader chunkHeader = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION, 9,
                87361L);

        final Bytes chunkBytes = chunkHeader.getBytes()
                .add(TestData.BYTES_9.paddedTo(DATABLOCK_PAYLOAD_SIZE));
        DataBlockPayload dataBlockPayload = DataBlockPayload.of(chunkBytes);
        DataBlockHeader dataBlockHeader = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        final DataBlock dataBlock = DataBlock.of(
                dataBlockHeader.toBytes().add(chunkBytes),
                DataBlockPosition.of(0));

        when(dataBlockReader.read()).thenReturn(dataBlock);

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> reader.read());

        // Verify that the read method returns the expected chunk.
        assertEquals(
                "Invalid chunk CRC, expected: '87361', actual: '3540561586'",
                e.getMessage());
    }

    @Test
    void test_read_of_last_chunk() {
        final ChunkHeader chunkHeader = ChunkHeader.of(Chunk.MAGIC_NUMBER,
                VERSION, 9, TestData.PAYLOAD_9.calculateCrc());
        final Bytes chunkBytes = chunkHeader.getBytes()
                .add(TestData.BYTES_9.paddedTo(DATABLOCK_PAYLOAD_SIZE));
        DataBlockPayload dataBlockPayload = DataBlockPayload.of(chunkBytes);
        DataBlockHeader dataBlockHeader = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        final DataBlock dataBlock = DataBlock.of(
                dataBlockHeader.toBytes().add(chunkBytes),
                DataBlockPosition.of(0));

        when(dataBlockReader.read()).thenReturn(dataBlock, (DataBlock) null);

        assertNotNull(reader.read());
        assertNull(reader.read());
        assertNull(reader.read());
        assertNull(reader.read());

    }

}
