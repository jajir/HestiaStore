package com.coroptis.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class HashTest {
    @Test
    void testStore_simple() throws Exception {
        Hash hash = new Hash(new BitArray(10),3);
        
        boolean ret = hash.store("ahoj".getBytes());

        assertTrue(ret);
    }
        @Test
    void testIsNotStored_simple() throws Exception {
        Hash hash = new Hash(new BitArray(10),10);

        assertTrue(hash.isNotStored("ahoj".getBytes()));
        hash.store("ahoj".getBytes());
        assertFalse(hash.isNotStored("ahoj".getBytes()));
        assertTrue(hash.isNotStored("kachna".getBytes()));
    }


        @Test
    void testConstructor_InvalidNumberOfHashFunctions() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new Hash(new BitArray(10),0));
    }

}
