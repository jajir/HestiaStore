package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class TypeIoTest {

    @Test
    void readFullyOrNull_returnsFalseWhenNoDataAreAvailable() {
        final boolean wasRead = TypeIo.readFullyOrNull(
                new MemFileReader(new byte[0]), new byte[4]);

        assertFalse(wasRead);
    }

    @Test
    void readFullyOrNull_readsAllRequestedBytes() {
        final byte[] destination = new byte[3];

        final boolean wasRead = TypeIo.readFullyOrNull(
                new MemFileReader(new byte[] { 1, (byte) 0xFF, 3 }),
                destination);

        assertTrue(wasRead);
        assertArrayEquals(new byte[] { 1, (byte) 0xFF, 3 }, destination);
    }

    @Test
    void readFullyOrNull_throwsOnTruncatedData() {
        final IndexException error = assertThrows(IndexException.class,
                () -> TypeIo.readFullyOrNull(new MemFileReader(new byte[] { 1,
                        2, 3 }), new byte[4]));

        assertTrue(error.getMessage().contains("Expected '4' bytes"));
        assertTrue(error.getMessage().contains("EOF"));
    }

    @Test
    void readFullyRequired_throwsWhenNoDataAreAvailable() {
        final IndexException error = assertThrows(IndexException.class,
                () -> TypeIo.readFullyRequired(new MemFileReader(new byte[0]),
                        new byte[1]));

        assertTrue(error.getMessage().contains("before reading any data"));
    }
}
