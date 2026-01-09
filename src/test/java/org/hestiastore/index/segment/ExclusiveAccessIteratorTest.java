package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.Test;

class ExclusiveAccessIteratorTest {

    @Test
    void constructor_rejects_null_delegate() {
        final SegmentStateMachine stateMachine = new SegmentStateMachine();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> new ExclusiveAccessIterator<>(null, stateMachine));

        assertEquals("Property 'delegate' must not be null.", ex.getMessage());
    }

    @Test
    void constructor_rejects_null_state_machine() {
        final EntryIterator<Integer, String> delegate = EntryIterator
                .make(List.<Entry<Integer, String>>of().iterator());

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> new ExclusiveAccessIterator<>(delegate, null));

        assertEquals("Property 'stateMachine' must not be null.",
                ex.getMessage());
    }

    @Test
    void close_returns_segment_to_ready_even_when_delegate_fails() {
        final SegmentStateMachine stateMachine = new SegmentStateMachine();
        assertTrue(stateMachine.tryEnterFreeze());
        final EntryIterator<Integer, String> delegate = new FailingCloseIterator<>();
        final ExclusiveAccessIterator<Integer, String> iterator = new ExclusiveAccessIterator<>(
                delegate, stateMachine);

        final Exception ex = assertThrows(RuntimeException.class,
                iterator::close);

        assertEquals("close failed", ex.getMessage());
        assertEquals(SegmentState.READY, stateMachine.getState());
    }

    @Test
    void delegates_iteration_calls() {
        final SegmentStateMachine stateMachine = new SegmentStateMachine();
        assertTrue(stateMachine.tryEnterFreeze());
        final Entry<Integer, String> entry = Entry.of(1, "one");
        final EntryIterator<Integer, String> delegate = EntryIterator
                .make(List.of(entry).iterator());

        final ExclusiveAccessIterator<Integer, String> iterator = new ExclusiveAccessIterator<>(
                delegate, stateMachine);

        assertTrue(iterator.hasNext());
        assertEquals(entry, iterator.next());
        iterator.close();
        assertEquals(SegmentState.READY, stateMachine.getState());
    }

    private static final class FailingCloseIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<K, V> next() {
            throw new UnsupportedOperationException("no entries");
        }

        @Override
        protected void doClose() {
            throw new RuntimeException("close failed");
        }
    }
}
