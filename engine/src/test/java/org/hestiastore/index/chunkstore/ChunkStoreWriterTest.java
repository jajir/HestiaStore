package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChunkStoreWriterTest {
    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private ChunkStoreWriter legacyWriter;

    @Test
    void legacyWriterExplicitlyRejectsUnsupportedPreparedChunks() {
        final ChunkData chunk = ChunkData.ofSequence(0, 0,
                ChunkHeader.MAGIC_NUMBER, 2, ByteSequence.EMPTY);
        assertThrows(IndexException.class,
                () -> legacyWriter.writePreparedChunk(chunk));
    }
}
