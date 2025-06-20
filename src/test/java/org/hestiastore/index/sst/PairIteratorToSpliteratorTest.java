package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PairIteratorToSpliteratorTest {

    private static final Pair<String, Integer> PAIR1 = Pair.of("aaa", 1);

    private static final TypeDescriptor<String> STRING_TD = new TypeDescriptorString();

    @Mock
    private PairIterator<String, Integer> pairIterator;

    @Test
    void test_required_pairIterator() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new PairIteratorToSpliterator<>(null, STRING_TD));

        assertEquals("Property 'pairIterator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_required_keyDescriptor() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new PairIteratorToSpliterator<>(pairIterator, null));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                e.getMessage());
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
