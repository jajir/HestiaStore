package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PairIteratorReaderTest {

    @Mock
    private PairReader<Integer, String> reader;

    @Mock
    private OptimisticLockObjectVersionProvider provider;

    @Test
    void test_without_lock() throws Exception {
        when(reader.read())//
                .thenReturn(Pair.of(1, "aaa")) //
                .thenReturn(Pair.of(2, "bbb"))//
                .thenReturn(Pair.of(3, "ccc"))//
                .thenReturn(null);
        final PairIterator<Integer, String> iterator = new PairIteratorReader<>(
                reader);

        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(1, "aaa"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(2, "bbb"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(3, "ccc"), iterator.next());
        assertFalse(iterator.hasNext());
        
        iterator.close();
    }

    @Test
    void test_empty_reader() throws Exception {
        when(reader.read())//
                .thenReturn(null);
        final PairIterator<Integer, String> iterator = new PairIteratorReader<>(
                reader);

        assertFalse(iterator.hasNext());
        
        iterator.close();
    }

    @Test
    void test_with_lock_unlocked() throws Exception {
        when(reader.read())//
                .thenReturn(Pair.of(1, "aaa")) //
                .thenReturn(Pair.of(2, "bbb"))//
                .thenReturn(Pair.of(3, "ccc"))//
                .thenReturn(null);
        when(provider.getVersion()).thenReturn(4,4,4,4,4,4);
        final OptimisticLock lock = new OptimisticLock(provider);
        final PairIterator<Integer, String> iterator = new PairIteratorReader<>(
                reader,lock);

        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(1, "aaa"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(2, "bbb"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(3, "ccc"), iterator.next());
        assertFalse(iterator.hasNext());
        
        iterator.close();
    }

    @Test
    void test_with_lock_locked_during_work() throws Exception {
        when(reader.read())//
                .thenReturn(Pair.of(1, "aaa")) //
                .thenReturn(Pair.of(2, "bbb"))//
                .thenReturn(Pair.of(3, "ccc"))//
                .thenReturn(null);
        when(provider.getVersion()).thenReturn(4,4,5,5,5);
        final OptimisticLock lock = new OptimisticLock(provider);
        final PairIterator<Integer, String> iterator = new PairIteratorReader<>(
                reader,lock);

        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(1, "aaa"), iterator.next());
        assertFalse(iterator.hasNext());
        
        iterator.close();
    }

    @Test
    void test_with_lock_locked() throws Exception {
        when(reader.read())//
                .thenReturn(Pair.of(1, "aaa")) //
                .thenReturn(Pair.of(2, "bbb"))//
                .thenReturn(Pair.of(3, "ccc"))//
                .thenReturn(null);
        when(provider.getVersion()).thenReturn(4,3,3,3,3);
        final OptimisticLock lock = new OptimisticLock(provider);
        final PairIterator<Integer, String> iterator = new PairIteratorReader<>(
                reader,lock);

        assertFalse(iterator.hasNext());
        
        iterator.close();
    }

}
