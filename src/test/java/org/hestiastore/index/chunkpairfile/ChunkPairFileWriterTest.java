package org.hestiastore.index.chunkpairfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Pair;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkPairFileWriterTest {

    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);
    @Mock
    private ChunkStoreWriter chunkStoreWriter;

    private TypeDescriptor<String> keyTypeDescriptor = new TypeDescriptorString();

    private TypeDescriptor<Long> valueTypeDescriptor = new TypeDescriptorLong();

    private ChunkPairFileWriter<String, Long> writer;

    @BeforeEach
    void beforeEach() {
        writer = new ChunkPairFileWriter<>(chunkStoreWriter, keyTypeDescriptor,
                valueTypeDescriptor);
    }

    @AfterEach
    void tearDown() {
        writer = null;
    }

    @Test
    void test_close_without_writing() {
        writer.close();
        verify(chunkStoreWriter, times(0)).write(any(), anyInt());
        verify(chunkStoreWriter, times(1)).close();
    }

    @Test
    void test_basic_write() {
        when(chunkStoreWriter.write(any(), anyInt()))
                .thenReturn(CellPosition.of(DATA_BLOCK_SIZE, 2048));
        writer.write(Pair.of("key1", 1L));
        CellPosition ret = writer.flush();

        assertEquals(2048, ret.getValue());
        verify(chunkStoreWriter, times(1)).write(any(), anyInt());
    }

}
