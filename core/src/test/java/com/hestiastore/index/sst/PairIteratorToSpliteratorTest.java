package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.datatype.TypeDescriptorString;

@ExtendWith(MockitoExtension.class)
public class PairIteratorToSpliteratorTest {

    private final static Pair<String, Integer> PAIR1 = Pair.of("aaa", 1);

    private final static TypeDescriptor<String> STRING_TD = new TypeDescriptorString();

    @Mock
    private PairIterator<String, Integer> pairIterator;

    @Test
    void test_required_pairIterator() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new PairIteratorToSpliterator<>(null, STRING_TD));

        assertEquals("Pair iterator is required", e.getMessage());
    }

    @Test
    void test_required_keyDescriptor() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new PairIteratorToSpliterator<>(pairIterator, null));

        assertEquals("Key type descriptor must not be null", e.getMessage());
    }

    @Test
    void test_tryAdvance() {
        when(pairIterator.hasNext()).thenReturn(true, false);
        when(pairIterator.next()).thenReturn(PAIR1);
        final PairIteratorToSpliterator<String, Integer> pairIteratorToSpliterator = new PairIteratorToSpliterator<>(
                pairIterator, STRING_TD);

        pairIteratorToSpliterator.tryAdvance(pair -> {
            assertSame(PAIR1, pair);
        });
        pairIteratorToSpliterator.tryAdvance(pair -> {
            fail();
        });
    }

}
