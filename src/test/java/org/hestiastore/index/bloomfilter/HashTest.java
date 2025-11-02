package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.bytes.Bytes;
import org.junit.jupiter.api.Test;

class HashTest {
    @Test
    void testStore_simple() {
        Hash hash = new Hash(new BitArray(10), 3);

        assertTrue(hash.store(Bytes.of("ahoj".getBytes())));
        assertFalse(hash.store(Bytes.of("ahoj".getBytes())));
        assertFalse(hash.store(Bytes.of("ahoj".getBytes())));
    }

    @Test
    void testStore_null_data() {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class, () -> hash.store(null));
    }

    @Test
    void testStore_zero_data() {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.store(Bytes.of(new byte[0])));
    }

    @Test
    void testIsNotStored_null_data() {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.isNotStored(null));
    }

    @Test
    void testIsNotStored_zero_data() {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.isNotStored(Bytes.of(new byte[0])));
    }

    @Test
    void testIsNotStored_simple() {
        Hash hash = new Hash(new BitArray(10), 10);

        assertTrue(hash.isNotStored(Bytes.of("ahoj".getBytes())));
        hash.store(Bytes.of("ahoj".getBytes()));
        assertFalse(hash.isNotStored(Bytes.of("ahoj".getBytes())));
        assertTrue(hash.isNotStored(Bytes.of("kachna".getBytes())));
    }

    @Test
    void testSmallSize() {
        Hash hash = new Hash(new BitArray(1), 1);

        hash.store(Bytes.of("a".getBytes()));
        hash.store(Bytes.of("b".getBytes()));
        hash.store(Bytes.of("c".getBytes()));
        hash.store(Bytes.of("d".getBytes()));

        // I'm sure this group is in index
        assertFalse(hash.isNotStored(Bytes.of("a".getBytes())));
        assertFalse(hash.isNotStored(Bytes.of("b".getBytes())));
        assertFalse(hash.isNotStored(Bytes.of("c".getBytes())));
        assertFalse(hash.isNotStored(Bytes.of("d".getBytes())));

        // this group have false positive
        assertTrue(hash.isNotStored(Bytes.of("e".getBytes())));
        assertTrue(hash.isNotStored(Bytes.of("f".getBytes())));
        assertTrue(hash.isNotStored(Bytes.of("g".getBytes())));
        assertFalse(hash.isNotStored(Bytes.of("h".getBytes())));
        assertTrue(hash.isNotStored(Bytes.of("i".getBytes())));
        assertFalse(hash.isNotStored(Bytes.of("j".getBytes())));
    }

    @Test
    void testConstructor_InvalidNumberOfHashFunctions() {
        final BitArray bitArray = new BitArray(10);
        assertThrows(IllegalArgumentException.class,
                () -> new Hash(bitArray, 0));
    }

}
