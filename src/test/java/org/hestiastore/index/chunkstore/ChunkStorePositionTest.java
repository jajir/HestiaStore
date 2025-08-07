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

    @Test
    void testAddDataBlock() {
        ChunkStorePosition position = ChunkStorePosition.of(1000)
                .addDataBlock(1000);
        assertEquals(2000, position.getValue());
    }

    @Test
    void test_addCellsForBytes_from0() {
        ChunkStorePosition position = ChunkStorePosition.of(0);
        assertEquals(16, position.addCellsForBytes(16).getValue());
        assertEquals(32, position.addCellsForBytes(17).getValue());
        assertEquals(48, position.addCellsForBytes(41).getValue());
    }

    @Test
    void test_addCellsForBytes_from1000() {
        ChunkStorePosition position = ChunkStorePosition.of(1000);
        assertEquals(1016, position.addCellsForBytes(16).getValue());
        assertEquals(1032, position.addCellsForBytes(17).getValue());
    }

}
