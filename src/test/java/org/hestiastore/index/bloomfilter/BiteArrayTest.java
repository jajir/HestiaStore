package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BiteArrayTest {

    private static final int BIT_ARRAY_SIZE = 10;
    private static final int TESTED_BYTE = 7;
    private static final int BITS_IN_BYTE = 8;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    void test_setBit_initilized_to1() {
        // Arrange
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);

        for (int i = 0; i < BITS_IN_BYTE; i++) {
            final int index = TESTED_BYTE * BITS_IN_BYTE + i;
            assertTrue(bitArray.setBit(index));
        }
        log(bitArray);

        for (int i = 0; i < BITS_IN_BYTE; i++) {
            final int index = TESTED_BYTE * BITS_IN_BYTE + i;
            assertTrue(bitArray.get(index),
                    "Bit at index " + index + " should be set");
        }
        log(bitArray);
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
        log(bitArray);
    }

    @Test
    void test_set_byte_minus() {
        // show at debug log -12 as binary
        byte[] b = new byte[1];
        b[0] = -12;
        logger.debug(toBinaryString(ByteSequences.wrap(b)));

        // set firts byte to -12
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);
        assertTrue(bitArray.setBit(2));
        assertTrue(bitArray.setBit(4));
        assertTrue(bitArray.setBit(5));
        assertTrue(bitArray.setBit(6));
        assertTrue(bitArray.setBit(7));

        log(bitArray);

        // verify that first byte is -12
        assertEquals(-12, bitArray.getBytes().getByte(0));
    }

    @Test
    void test_setBit_notInitialized() {
        BitArray bitArray = new BitArray(BIT_ARRAY_SIZE);

        for (int i = 0; i < BITS_IN_BYTE; i++) {
            final int index = TESTED_BYTE * BITS_IN_BYTE + i;
            assertTrue(bitArray.setBit(index));
            assertTrue(bitArray.get(index),
                    "Bit at index " + index + " should be set");
            assertFalse(bitArray.setBit(index));
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
        assertEquals((byte) 0b00100000, bitArray.getBytes().getByte(0));
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
        log(bitArray);
        assertEquals((byte) 0b00100000, bitArray.getBytes().getByte(0));
        assertTrue(result);
    }

    @Test
    void testGet_doesnt_change_value_0() {
        // Arrange
        BitArray bitArray = new BitArray(10);

        // Act
        boolean result = bitArray.get(5);

        // Assert
        log(bitArray);
        assertEquals((byte) 0b00000000, bitArray.getBytes().getByte(0));
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
        assertEquals(bitArray, bitArray);
    }

    @Test
    void testEquals_differentClass() {
        // Arrange
        BitArray bitArray = new BitArray(10);
        Object obj = new Object();

        // Act and Assert
        assertNotEquals(bitArray, obj);
    }

    @Test
    void testEquals_equalArrays() {
        // Arrange
        BitArray bitArray1 = new BitArray(10);
        BitArray bitArray2 = new BitArray(10);

        // Act and Assert
        assertEquals(bitArray1, bitArray2);
        assertEquals(bitArray2, bitArray1);
    }

    @Test
    void testEquals_unequalArrays() {
        // Arrange
        BitArray bitArray1 = new BitArray(10);
        BitArray bitArray2 = new BitArray(5);

        // Act and Assert
        assertNotEquals(bitArray1, bitArray2);
        assertNotEquals(bitArray2, bitArray1);
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

    private void log(final BitArray value) {
        logger.debug(toBinaryString(value.getBytes()));
    }

    private String toBinaryString(final ByteSequence data) {
        StringBuilder sb = new StringBuilder();
        for (byte b : data.toByteArray()) {
            sb.append(String.format("%8s", Integer.toBinaryString(b & 0xFF))
                    .replace(' ', '0'));
        }
        return sb.toString();
    }

}
