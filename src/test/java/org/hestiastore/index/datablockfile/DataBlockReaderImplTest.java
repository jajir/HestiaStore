package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.TestData;
import org.hestiastore.index.directory.FileReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DataBlockReaderImplTest {

    private static final int BLOCK_SIZE = 1024;

    private static final DataBlockPosition BLOCK_POSITION = DataBlockPosition
            .of(2048);

    @Mock
    private FileReader fileReader;

    private DataBlockReaderImpl reader;

    @BeforeEach
    void beforeEach() {
        reader = new DataBlockReaderImpl(fileReader, BLOCK_POSITION,
                BLOCK_SIZE);
    }

    @AfterEach
    void afterEach() {
        reader.close();
    }

    @Test
    void test_read() {
        byte[] bufferBytes = new byte[1024];
        System.arraycopy(TestData.BYTE_ARRAY_1024, 0, bufferBytes, 0, 1024);
        DataBlockHeader header = DataBlockHeader.of(DataBlock.MAGIC_NUMBER,
                2131L);
        System.arraycopy(header.toBytes().getData(), 0, bufferBytes, 0, 16);

        when(fileReader.read(any(byte[].class))).thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            assertEquals(1024, buffer.length);
            System.arraycopy(bufferBytes, 0, buffer, 0, 1024);
            return 1024;
        });
        DataBlock ret1 = reader.read();
        assertNotNull(ret1);
        assertTrue(Arrays.equals(bufferBytes, ret1.getBytes().getData()));
        assertEquals(2048, ret1.getPosition().getValue());
    }

    @Test
    void test_read_invalidBlockSize_was_readed() {
        when(fileReader.read(any(byte[].class))).thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            assertEquals(1024, buffer.length);
            System.arraycopy(TestData.BYTE_ARRAY_1024, 0, buffer, 0, 45);
            return 45;
        });
        final Exception e = assertThrows(IndexException.class,
                () -> reader.read());

        assertEquals("Unable to read full block", e.getMessage());
    }

    @Test
    void test_propagateException() {
        when(fileReader.read(any(byte[].class)))
                .thenThrow(new IndexException("Test Exception"));

        assertThrows(IndexException.class, () -> reader.read());
    }

}
