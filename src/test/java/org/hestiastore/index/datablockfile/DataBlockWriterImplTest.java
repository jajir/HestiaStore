package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.directory.FileWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DataBlockWriterImplTest {

    private static final DataBlockSize BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    @Mock
    private FileWriter fileWriter;

    private DataBlockWriterImpl writer;

    @BeforeEach
    void beforeEach() {
        writer = new DataBlockWriterImpl(fileWriter, BLOCK_SIZE);
    }

    @AfterEach
    void afterEach() {
        writer.close();
    }

    @Test
    void test_write() {
        doAnswer(invocation -> {
            final Bytes blockData = invocation.getArgument(0, Bytes.class);
            assertEquals(1024, blockData.length());

            // Verify the magic number
            final byte[] longBytes = new byte[8];
            blockData.copyTo(0, longBytes, 0, longBytes.length);
            long magicNumber = TestData.LONG_CONVERTOR_FROM_BYTES
                    .fromBytes(Bytes.of(longBytes));
            assertEquals(DataBlockHeader.MAGIC_NUMBER, magicNumber);

            // Verify the CRC
            final byte[] crcBytes = new byte[8];
            blockData.copyTo(8, crcBytes, 0, crcBytes.length);
            long crc = TestData.LONG_CONVERTOR_FROM_BYTES
                    .fromBytes(Bytes.of(crcBytes));
            assertEquals(TestData.PAYLOAD_1008.calculateCrc(), crc);

            return null;
        }).when(fileWriter).write(any(Bytes.class));
        writer.write(TestData.PAYLOAD_1008);
    }

    @Test
    void test_write_invalidSize() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> writer.write(TestData.PAYLOAD_1024));

        assertEquals(
                "Payload size '1024' does not match expected payload size '1008'",
                e.getMessage());
    }

}
