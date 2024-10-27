package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
public class PairIteratorWithLockTest {

    private final static SegmentId SEGMENT_ID = SegmentId.of(27);

    @Mock
    private PairIteratorWithLock<String, Integer> iter;

    @Mock
    private OptimisticLock lock;

    private PairIterator<String, Integer> iterator;

    @BeforeEach
    void setUp() {
        iterator = new PairIteratorWithLock<>(iter, lock,
                SEGMENT_ID.toString());
    }

    @Test
    void test_is_locked() throws Exception {
        when(lock.isLocked()).thenReturn(true);
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_unlocked_inner_in_not_next() throws Exception {
        when(lock.isLocked()).thenReturn(false);
        when(iter.hasNext()).thenReturn(false);
        
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_unlocked_inner_in_next() throws Exception {
        when(lock.isLocked()).thenReturn(false);
        when(iter.hasNext()).thenReturn(true);
        
        assertTrue(iterator.hasNext());
    }

    @Test
    void test_try_to_move_next_in_locked() throws Exception {
        when(lock.isLocked()).thenReturn(true);
        
        final Exception e = assertThrows(NoSuchElementException.class, ()->{iterator.next();});
        
        assertEquals("Unable to move to next element in iterator"
                + " 'segment-00027' because it's locked.",e.getMessage());
    }

}
