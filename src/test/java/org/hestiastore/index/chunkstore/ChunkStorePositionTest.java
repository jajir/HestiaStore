package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ChunkStorePositionTest {

    @Test
    void test() {
        ChunkStorePosition position = ChunkStorePosition.of(1000);
        assertEquals(62, position.toDataBlockPosition().getValue());
        assertEquals(8, position.cellIndex());
    }

}
