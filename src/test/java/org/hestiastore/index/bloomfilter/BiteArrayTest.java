package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class BiteArrayTest {

    private static final int BIT_ARRAY_SIZE = 10;
    private static final int TESTED_BYTE = 7;
    private static final int BITS_IN_BYTE = 8;

    @Test
    void test_setBit_initilized_to1() {
        // Arrange
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);

        for (int i = 0; i < BITS_IN_BYTE; i++) {
            final int index = TESTED_BYTE * BITS_IN_BYTE + i;
            assertTrue(bitArray.setBit(index));
        }

        for (int i = 0; i < BITS_IN_BYTE; i++) {
            final int index = TESTED_BYTE * BITS_IN_BYTE + i;
            assertTrue(bitArray.get(index),
                    "Bit at index " + index + " should be set");
        }
    }

    @Test
    void test_setBit_initilized_to0() {
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);

        bitArray.setBit(30 + 5);

        assertFalse(bitArray.get(30 + 0));
        assertFalse(bitArray.get(30 + 1));
        assertFalse(bitArray.get(30 + 2));
        assertFalse(bitArray.get(30 + 3));
        assertFalse(bitArray.get(30 + 4));
        assertTrue(bitArray.get(30 + 5));
        assertFalse(bitArray.get(30 + 6));
        assertFalse(bitArray.get(30 + 7));
    }

    @Test
    void test_setBit_notInitialized() {
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);

        for (int i = 0; i < BITS_IN_BYTE; i++) {
            final int index = TESTED_BYTE * BITS_IN_BYTE + i;
            assertTrue(bitArray.setBit(index));
            assertTrue(bitArray.get(index),
                    "Bit at index " + index + " should be set");
        }
    }

    @Test
    void test_array_is_set_to_0() {
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);

        for (int index = 0; index < BITS_IN_BYTE * BIT_ARRAY_SIZE; index++) {
            assertFalse(bitArray.get(index),
                    "Bit at index " + index + " should be set");
        }
    }

    @Test
    void testSetBit_validIndex() {
        // Arrange
        BitArray bitArray = new BitArray(10);

        // Act
        boolean result = bitArray.setBit(5);

        // Assert
        assertTrue(result);
        assertEquals((byte) 0b00100000, bitArray.getByteArray()[0]);
    }

    @Test
    void testSetBit_invalidIndex() {
        // Arrange
        BitArray bitArray = new BitArray(10);

        // Act and Assert
        assertThrows(IndexOutOfBoundsException.class,
                () -> bitArray.setBit(-1));
    }

    @Test
    void testGet_doesnt_change_value_1() {
        // Arrange
        BitArray bitArray = new BitArray(10);
        bitArray.setBit(5);

        // Act
        boolean result = bitArray.get(5);

        // Assert
        assertEquals((byte) 0b00100000, bitArray.getByteArray()[0]);
        assertTrue(result);
    }

    @Test
    void testGet_doesnt_change_value_0() {
        // Arrange
        BitArray bitArray = new BitArray(10);

        // Act
        boolean result = bitArray.get(5);

        // Assert
        assertEquals((byte) 0b00000000, bitArray.getByteArray()[0]);
        assertFalse(result);
    }

    @Test
    void testGet_invalidIndex() {
        // Arrange
        BitArray bitArray = new BitArray(10);

        // Act and Assert
        assertThrows(IndexOutOfBoundsException.class, () -> bitArray.get(-1));
    }

    @Test
    void testEquals_sameInstance() {
        // Arrange
        BitArray bitArray = new BitArray(10);

        // Act and Assert
        assertTrue(bitArray.equals(bitArray));
    }

    @Test
    void testEquals_differentClass() {
        // Arrange
        BitArray bitArray = new BitArray(10);
        Object obj = new Object();

        // Act and Assert
        assertFalse(bitArray.equals(obj));
    }

    @Test
    void testEquals_equalArrays() {
        // Arrange
        BitArray bitArray1 = new BitArray(10);
        BitArray bitArray2 = new BitArray(10);

        // Act and Assert
        assertTrue(bitArray1.equals(bitArray2));
        assertTrue(bitArray2.equals(bitArray1));
    }

    @Test
    void testEquals_unequalArrays() {
        // Arrange
        BitArray bitArray1 = new BitArray(10);
        BitArray bitArray2 = new BitArray(5);

        // Act and Assert
        assertFalse(bitArray1.equals(bitArray2));
        assertFalse(bitArray2.equals(bitArray1));
    }

    @Test
    void testHashCode_equalArrays() {
        // Arrange
        BitArray bitArray1 = new BitArray(10);
        BitArray bitArray2 = new BitArray(10);

        // Act and Assert
        assertEquals(bitArray1.hashCode(), bitArray2.hashCode());
    }

    @Test
    void testHashCode_unequalArrays() {
        // Arrange
        BitArray bitArray1 = new BitArray(10);
        BitArray bitArray2 = new BitArray(5);

        // Act and Assert
        assertNotEquals(bitArray1.hashCode(), bitArray2.hashCode());
    }
}
