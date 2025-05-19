package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

@ExtendWith(MockitoExtension.class)
public class PairReaderFromIteratorTest {

    private final static Pair<String, Integer> PAIR1 = Pair.of("aaa", 1);

    @Mock
    private PairIterator<String, Integer> pairIterator;

    @Test
    void test_iteratorIsRequired() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new PairReaderFromIterator<String, Integer>(null));

        assertEquals("Pair iterator cannot be null", e.getMessage());

    }

    @Test
    void test_one_record() {
        when(pairIterator.hasNext()).thenReturn(true, false);
        when(pairIterator.next()).thenReturn(PAIR1);
        final PairReaderFromIterator<String, Integer> reader = new PairReaderFromIterator<>(
                pairIterator);

        assertSame(PAIR1, reader.read());
        assertSame(null, reader.read());
    }

}
