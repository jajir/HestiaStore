package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.blockdatafile.DataBlockPayload;
import org.hestiastore.index.blockdatafile.DataBlockWriter;
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

    private static final ChunkPayload PAYLOAD_9 = ChunkPayload
            .of(Bytes.of("test data".getBytes()));

    private static final ChunkPayload PAYLOAD_15 = ChunkPayload
            .of(Bytes.of("super test data".getBytes()));

    private static final ChunkPayload PAYLOAD_154 = ChunkPayload
            .of(Bytes.of(TestData.BYTE_ARRAY_154));

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Mock
    private DataBlockWriter dataBlockWriter;

    private ChunkStoreWriterImpl writer;

    @BeforeEach
    void beforeEach() {
        writer = new ChunkStoreWriterImpl(dataBlockWriter, DATABLOCK_SIZE,
                DATABLOCK_PAYLOAD_SIZE);
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
        ChunkStorePosition position = writer.write(PAYLOAD_154, VERSION);
        assertEquals(0, position.getValue());
        writer.close();

        final ChunkHeader header = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                PAYLOAD_154.length(), PAYLOAD_154.calculateCrc());
        Bytes bytesToWrite = Bytes.of(header.toBytes(), PAYLOAD_154.getBytes());
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
        ChunkStorePosition position1 = writer.write(PAYLOAD_9, VERSION);
        assertEquals(0, position1.getValue());
        writer.close();

        final ChunkHeader header1 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                PAYLOAD_9.length(), PAYLOAD_9.calculateCrc());
        Bytes bytesToWrite = Bytes.of(header1.toBytes(), PAYLOAD_9.getBytes())
                .paddedTo(64);

        logger.info("Bytes length: {}", bytesToWrite.length());

        verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite));
    }

    @Test
    void test_write_small_chunks() {
        ChunkStorePosition position1 = writer.write(PAYLOAD_9, VERSION);
        assertEquals(0, position1.getValue());
        ChunkStorePosition position2 = writer.write(PAYLOAD_15, VERSION);
        assertEquals(48, position2.getValue());
        writer.close();

        final ChunkHeader header1 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                PAYLOAD_9.length(), PAYLOAD_9.calculateCrc());
        Bytes bytesToWrite1 = Bytes.of(header1.toBytes(), PAYLOAD_9.getBytes())
                .paddedTo(48);

        final ChunkHeader header2 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                PAYLOAD_15.length(), PAYLOAD_15.calculateCrc());
        bytesToWrite1 = bytesToWrite1.add(header2.toBytes());
        bytesToWrite1 = bytesToWrite1.add(PAYLOAD_15.getBytes());

        System.out.println(bytesToWrite1.length());

        verify(dataBlockWriter).write(DataBlockPayload.of(bytesToWrite1));
        logger.info("Bytes length: {}", bytesToWrite1.length());
    }

}
