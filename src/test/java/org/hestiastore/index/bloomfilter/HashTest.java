package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class HashTest {
    @Test
    void testStore_simple() {
        Hash hash = new Hash(new BitArray(10), 3);

        assertTrue(hash.store(ByteSequences.wrap("ahoj".getBytes())));
        assertFalse(hash.store(ByteSequences.wrap("ahoj".getBytes())));
        assertFalse(hash.store(ByteSequences.wrap("ahoj".getBytes())));
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
                () -> hash.store(ByteSequences.wrap(new byte[0])));
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
                () -> hash.isNotStored(ByteSequences.wrap(new byte[0])));
    }

    @Test
    void testIsNotStored_simple() {
        Hash hash = new Hash(new BitArray(10), 10);

        assertTrue(hash.isNotStored(ByteSequences.wrap("ahoj".getBytes())));
        hash.store(ByteSequences.wrap("ahoj".getBytes()));
        assertFalse(hash.isNotStored(ByteSequences.wrap("ahoj".getBytes())));
        assertTrue(hash.isNotStored(ByteSequences.wrap("kachna".getBytes())));
    }

    @Test
    void testSmallSize() {
        Hash hash = new Hash(new BitArray(1), 1);

        hash.store(ByteSequences.wrap("a".getBytes()));
        hash.store(ByteSequences.wrap("b".getBytes()));
        hash.store(ByteSequences.wrap("c".getBytes()));
        hash.store(ByteSequences.wrap("d".getBytes()));

        // I'm sure this group is in index
        assertFalse(hash.isNotStored(ByteSequences.wrap("a".getBytes())));
        assertFalse(hash.isNotStored(ByteSequences.wrap("b".getBytes())));
        assertFalse(hash.isNotStored(ByteSequences.wrap("c".getBytes())));
        assertFalse(hash.isNotStored(ByteSequences.wrap("d".getBytes())));

        // this group have false positive
        assertTrue(hash.isNotStored(ByteSequences.wrap("e".getBytes())));
        assertTrue(hash.isNotStored(ByteSequences.wrap("f".getBytes())));
        assertTrue(hash.isNotStored(ByteSequences.wrap("g".getBytes())));
        assertFalse(hash.isNotStored(ByteSequences.wrap("h".getBytes())));
        assertTrue(hash.isNotStored(ByteSequences.wrap("i".getBytes())));
        assertFalse(hash.isNotStored(ByteSequences.wrap("j".getBytes())));
    }

    @Test
    void testConstructor_InvalidNumberOfHashFunctions() {
        final BitArray bitArray = new BitArray(10);
        assertThrows(IllegalArgumentException.class,
                () -> new Hash(bitArray, 0));
    }

}
