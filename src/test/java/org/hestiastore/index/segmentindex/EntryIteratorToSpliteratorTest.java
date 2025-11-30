package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntryIteratorToSpliteratorTest {

    private static final Entry<String, Integer> ENTRY1 = Entry.of("aaa", 1);

    private static final TypeDescriptor<String> STRING_TD = new TypeDescriptorShortString();

    @Mock
    private EntryIterator<String, Integer> entryIterator;

    @Test
    void test_required_entryIterator() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new EntryIteratorToSpliterator<>(null, STRING_TD));

        assertEquals("Property 'entryIterator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_required_keyDescriptor() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new EntryIteratorToSpliterator<>(entryIterator, null));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                e.getMessage());
    }

    @Test
    void test_tryAdvance() {
        when(entryIterator.hasNext()).thenReturn(true, false);
        when(entryIterator.next()).thenReturn(ENTRY1);
        final EntryIteratorToSpliterator<String, Integer> entryIteratorToSpliterator = new EntryIteratorToSpliterator<>(
                entryIterator, STRING_TD);

        entryIteratorToSpliterator.tryAdvance(entry -> {
            assertSame(ENTRY1, entry);
        });
        entryIteratorToSpliterator.tryAdvance(entry -> {
            fail();
        });
    }

}
