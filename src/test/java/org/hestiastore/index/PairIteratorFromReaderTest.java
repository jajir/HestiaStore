package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PairIteratorFromReaderTest {

    private static final Pair<Integer, String> PAIR_1 = Pair.of(1, "aaa");
    private static final Pair<Integer, String> PAIR_2 = Pair.of(2, "bbb");
    private static final Pair<Integer, String> PAIR_3 = Pair.of(3, "ccc");

    @Mock
    private CloseablePairReader<Integer, String> reader;

    @Mock
    private OptimisticLockObjectVersionProvider provider;

    @Test
    void test_iteartor() {
        when(reader.read())//
                .thenReturn(PAIR_1) //
                .thenReturn(PAIR_2)//
                .thenReturn(PAIR_3)//
                .thenReturn(null);
        final PairIterator<Integer, String> iterator = new PairIteratorFromReader<>(
                reader);

        assertTrue(iterator.hasNext());
        assertEquals(PAIR_1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(PAIR_2, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(PAIR_3, iterator.next());
        assertFalse(iterator.hasNext());

        iterator.close();
    }

    @Test
    void test_iteartor_with_current() {
        when(reader.read())//
                .thenReturn(PAIR_1) //
                .thenReturn(PAIR_2)//
                .thenReturn(PAIR_3)//
                .thenReturn(null);
        final PairIteratorFromReader<Integer, String> iterator = new PairIteratorFromReader<>(
                reader);

        assertTrue(iterator.getCurrent().isEmpty());
        assertTrue(iterator.hasNext());
        assertEquals(PAIR_1, iterator.next());
        assertTrue(iterator.getCurrent().isPresent());
        assertEquals(PAIR_1, iterator.getCurrent().get());

        assertTrue(iterator.hasNext());
        assertEquals(PAIR_2, iterator.next());
        assertTrue(iterator.getCurrent().isPresent());
        assertEquals(PAIR_2, iterator.getCurrent().get());

        assertTrue(iterator.hasNext());
        assertEquals(PAIR_3, iterator.next());
        assertTrue(iterator.getCurrent().isPresent());
        assertEquals(PAIR_3, iterator.getCurrent().get());

        assertFalse(iterator.hasNext());
        assertTrue(iterator.getCurrent().isPresent());
        assertEquals(PAIR_3, iterator.getCurrent().get());

        iterator.close();
    }

    @Test
    void test_empty_reader() {
        when(reader.read())//
                .thenReturn(null);
        final PairIterator<Integer, String> iterator = new PairIteratorFromReader<>(
                reader);

        assertFalse(iterator.hasNext());

        iterator.close();
    }

}
