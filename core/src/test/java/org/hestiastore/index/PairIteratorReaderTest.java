package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.OptimisticLockObjectVersionProvider;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorFromReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PairIteratorReaderTest {

    @Mock
    private CloseablePairReader<Integer, String> reader;

    @Mock
    private OptimisticLockObjectVersionProvider provider;

    @Test
    void test_without_lock() throws Exception {
        when(reader.read())//
                .thenReturn(Pair.of(1, "aaa")) //
                .thenReturn(Pair.of(2, "bbb"))//
                .thenReturn(Pair.of(3, "ccc"))//
                .thenReturn(null);
        final PairIterator<Integer, String> iterator = new PairIteratorFromReader<>(
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
        final PairIterator<Integer, String> iterator = new PairIteratorFromReader<>(
                reader);

        assertFalse(iterator.hasNext());

        iterator.close();
    }

}
