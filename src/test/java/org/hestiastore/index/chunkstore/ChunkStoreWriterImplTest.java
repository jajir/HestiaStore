package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.DataBlockPayload;
import org.hestiastore.index.datablockfile.DataBlockWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
public class ChunkStoreWriterImplTest {

    private static final int DATABLOCK_SIZE = 80;

    private static final int DATABLOCK_PAYLOAD_SIZE = 64;

    private static final int VERSION = 1;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Mock
    private DataBlockWriter dataBlockWriter;

    private ChunkStoreWriterOldImpl writer;

    @BeforeEach
    void beforeEach() {
        writer = new ChunkStoreWriterOldImpl(CellPosition.of(DATABLOCK_SIZE, 0),
                dataBlockWriter, DATABLOCK_PAYLOAD_SIZE);
    }

    @AfterEach
    void afterEach() {
        writer.close();
    }

    /**
     * test splutting large chunk into multiple data blocks.
     */
    @Test
    void test_write_large_chunk() {
        CellPosition position = writer.write(TestData.PAYLOAD_154, VERSION);
        assertEquals(0, position.getValue());
        writer.close();

        final ChunkHeader header = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                TestData.PAYLOAD_154.length(),
                TestData.PAYLOAD_154.calculateCrc());
        Bytes bytesToWrite = Bytes.of(header.getBytes(),
                TestData.PAYLOAD_154.getBytes());
        Bytes bytesToWrite0 = bytesToWrite.subBytes(0, 64);
        Bytes bytesToWrite1 = bytesToWrite.subBytes(64, 128);
        Bytes bytesToWrite2 = bytesToWrite.subBytes(128, 186).paddedTo(64);
        // Bytes bytesToWrite3 = bytesToWrite.subBytes(192, 256).paddedTo(64);

        logger.info("Bytes length: {}", bytesToWrite.length());
        verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite0));
        verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite1));
        verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite2));
        // verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite3));
    }

    @Test
    void test_write_small_chunk() {
        CellPosition position1 = writer.write(TestData.PAYLOAD_9, VERSION);
        assertEquals(0, position1.getValue());
        writer.close();

        final ChunkHeader header1 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                TestData.PAYLOAD_9.length(), TestData.PAYLOAD_9.calculateCrc());
        Bytes bytesToWrite = Bytes
                .of(header1.getBytes(), TestData.PAYLOAD_9.getBytes())
                .paddedTo(64);

        logger.info("Bytes length: {}", bytesToWrite.length());

        verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite));
    }

    @Test
    void test_write_small_chunks() {
        CellPosition position1 = writer.write(TestData.PAYLOAD_9, VERSION);
        assertEquals(0, position1.getValue());
        CellPosition position2 = writer.write(TestData.PAYLOAD_15, VERSION);
        assertEquals(48, position2.getValue());
        writer.close();

        final ChunkHeader header1 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                TestData.PAYLOAD_9.length(), TestData.PAYLOAD_9.calculateCrc());
        Bytes bytesToWrite = Bytes
                .of(header1.getBytes(), TestData.PAYLOAD_9.getBytes())
                .paddedTo(48);

        final ChunkHeader header2 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                TestData.PAYLOAD_15.length(),
                TestData.PAYLOAD_15.calculateCrc());
        bytesToWrite = bytesToWrite.add(header2.getBytes());
        bytesToWrite = bytesToWrite
                .add(TestData.PAYLOAD_15.getBytes().paddedTo(16));

        assertEquals(96, bytesToWrite.length());

        Bytes dataBlock1 = bytesToWrite.subBytes(0, 64);
        Bytes dataBlock2 = bytesToWrite.subBytes(64, 96).paddedTo(64);
        verify(dataBlockWriter).write(DataBlockPayload.of(dataBlock1));
        verify(dataBlockWriter).write(DataBlockPayload.of(dataBlock2));
    }

}
