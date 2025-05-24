package com.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class HashTest {
    @Test
    void testStore_simple() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertTrue(hash.store("ahoj".getBytes()));
        assertFalse(hash.store("ahoj".getBytes()));
        assertFalse(hash.store("ahoj".getBytes()));
    }

    @Test
    void testStore_null_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(NullPointerException.class, () -> hash.store(null));
    }

    @Test
    void testStore_zero_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.store(new byte[0]));
    }

    @Test
    void testIsNotStored_null_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(NullPointerException.class, () -> hash.isNotStored(null));
    }

    @Test
    void testIsNotStored_zero_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.isNotStored(new byte[0]));
    }

    @Test
    void testIsNotStored_simple() throws Exception {
        Hash hash = new Hash(new BitArray(10), 10);

        assertTrue(hash.isNotStored("ahoj".getBytes()));
        hash.store("ahoj".getBytes());
        assertFalse(hash.isNotStored("ahoj".getBytes()));
        assertTrue(hash.isNotStored("kachna".getBytes()));
    }

    @Test
    void testSmallSize() throws Exception {
        Hash hash = new Hash(new BitArray(1), 1);

        hash.store("a".getBytes());
        hash.store("b".getBytes());
        hash.store("c".getBytes());
        hash.store("d".getBytes());

        // I'm sure this group is in index
        assertFalse(hash.isNotStored("a".getBytes()));
        assertFalse(hash.isNotStored("b".getBytes()));
        assertFalse(hash.isNotStored("c".getBytes()));
        assertFalse(hash.isNotStored("d".getBytes()));

        // this group have false positive
        assertTrue(hash.isNotStored("e".getBytes()));
        assertTrue(hash.isNotStored("f".getBytes()));
        assertTrue(hash.isNotStored("g".getBytes()));
        assertFalse(hash.isNotStored("h".getBytes()));
        assertTrue(hash.isNotStored("i".getBytes()));
        assertFalse(hash.isNotStored("j".getBytes()));
    }

    @Test
    void testConstructor_InvalidNumberOfHashFunctions() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> new Hash(new BitArray(10), 0));
    }

}
