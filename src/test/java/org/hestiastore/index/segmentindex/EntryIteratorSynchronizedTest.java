package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.util.concurrent.locks.Lock;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntryIteratorSynchronizedTest {

    @Mock
    private EntryIterator<String, String> iterator;

    @Mock
    private Lock lock;

    @Test
    void locksAroundHasNext() {
        when(iterator.hasNext()).thenReturn(true);
        try (EntryIteratorSynchronized<String, String> synchronizedIterator = new EntryIteratorSynchronized<>(
                iterator, lock)) {
            assertTrue(synchronizedIterator.hasNext());

            final InOrder order = inOrder(lock, iterator);
            order.verify(lock).lock();
            order.verify(iterator).hasNext();
            order.verify(lock).unlock();
        }
    }

    @Test
    void locksAroundNext() {
        when(iterator.next()).thenReturn(Entry.of("key", "value"));
        try (EntryIteratorSynchronized<String, String> synchronizedIterator = new EntryIteratorSynchronized<>(
                iterator, lock)) {
            assertEquals(Entry.of("key", "value"), synchronizedIterator.next());

            final InOrder order = inOrder(lock, iterator);
            order.verify(lock).lock();
            order.verify(iterator).next();
            order.verify(lock).unlock();
        }
    }

    @Test
    void locksAroundClose() {
        final EntryIteratorSynchronized<String, String> synchronizedIterator = new EntryIteratorSynchronized<>(
                iterator, lock);

        synchronizedIterator.close();

        final InOrder order = inOrder(lock, iterator);
        order.verify(lock).lock();
        order.verify(iterator).close();
        order.verify(lock).unlock();
    }
}
