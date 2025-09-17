package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ChunkStorePositionTest {

    private static final int DATABLOCK_SIZE = 1024;

    @Test
    void test() {
        CellPosition position = CellPosition.of(DATABLOCK_SIZE, 1024);
        assertEquals(1, position.getDataBlockPosition().getValue());
        assertEquals(1, position.getCellIndex());
        assertEquals(1024, position.getValue());
    }

    @Test
    void testAddDataBlock() {
        CellPosition position = CellPosition.of(DATABLOCK_SIZE, 1000)
                .addDataBlock();
        assertEquals(2024, position.getValue());
    }

    @Test
    void test_addCellsForBytes_from0() {
        CellPosition position = CellPosition.of(DATABLOCK_SIZE, 0);
        assertEquals(16, position.addCellsForBytes(16).getValue());
        assertEquals(32, position.addCellsForBytes(17).getValue());
        assertEquals(48, position.addCellsForBytes(41).getValue());
    }

    @Test
    void test_addCellsForBytes_from1000() {
        CellPosition position = CellPosition.of(DATABLOCK_SIZE, 1000);
        assertEquals(1016, position.addCellsForBytes(16).getValue());
        assertEquals(1032, position.addCellsForBytes(17).getValue());
    }

    @Test
    void test_getCellIndex_256() {
        CellPosition position = CellPosition.of(DATABLOCK_SIZE, 1024 + 256);
        assertEquals(17, position.getCellIndex());
    }

    @Test
    void test_getCellIndex_1024() {
        CellPosition position = CellPosition.of(DATABLOCK_SIZE, 1024);
        assertEquals(1, position.getCellIndex());
    }

    @Test
    void test_getOccupiedBytes_1024() {
        CellPosition position = CellPosition.of(80, 64);
        assertEquals(0, position.getOccupiedBytes());
    }

}
