package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.junit.jupiter.api.Test;

class SenkuFlushOrderTest {

    @Test
    void explicitLongSetSetterRequiresPrimitiveKeysWithoutValueStorage() {
        final SenkuFlushOrder<Long, NullValue> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorNull(), 2);
        order.setLong(0, Long.MAX_VALUE);
        order.setLong(1, Long.MIN_VALUE);
        order.sort(0, 2);
        assertEquals(Long.MIN_VALUE, order.longKey(0));
        assertEquals(Long.MAX_VALUE, order.longKey(1));
        assertEquals(NULL, order.value(0));
        final SenkuFlushOrder<Long, Long> withValues = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorLong(), 1);
        assertThrows(IllegalStateException.class,
                () -> withValues.setLong(0, 1L));
        final SenkuFlushOrder<Integer, NullValue> generic = new SenkuFlushOrder<>(
                new TypeDescriptorInteger(), new TypeDescriptorNull(), 1);
        assertThrows(IllegalStateException.class, () -> generic.setLong(0, 1L));
    }

    @Test
    void primitiveLongOrderSortsSignedKeysAndRetainsValues() {
        final SenkuFlushOrder<Long, Integer> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorInteger(), 5);
        order.set(0, Long.MIN_VALUE, 1);
        order.set(1, 7L, 2);
        order.set(2, -1L, 3);
        order.set(3, 0L, 4);
        order.set(4, Long.MAX_VALUE, 5);

        order.sort(0, 5);

        assertTrue(order.hasPrimitiveLongKeys());
        assertEquals(Long.MIN_VALUE, order.longKey(0));
        assertEquals(1, order.value(0));
        assertEquals(-1L, order.longKey(1));
        assertEquals(3, order.value(1));
        assertEquals(0L, order.longKey(2));
        assertEquals(4, order.value(2));
        assertEquals(7L, order.longKey(3));
        assertEquals(2, order.value(3));
        assertEquals(Long.MAX_VALUE, order.longKey(4));
        assertEquals(5, order.value(4));
        assertThrows(IllegalStateException.class, () -> order.key(0));
    }

    @Test
    void genericOrderUsesDescriptorComparatorAndSortsOnlyRequestedRange() {
        final SenkuFlushOrder<Long, Integer> order = new SenkuFlushOrder<>(
                new UnsignedLongDescriptor(), new TypeDescriptorInteger(), 6);
        order.set(0, -2L, 10);
        order.set(1, -1L, 11);
        order.set(2, 0L, 12);
        order.set(3, Long.MIN_VALUE, 13);
        order.set(4, Long.MAX_VALUE, 14);
        order.set(5, 7L, 15);

        order.sort(1, 5);

        assertFalse(order.hasPrimitiveLongKeys());
        assertEquals(-2L, order.key(0));
        assertEquals(0L, order.key(1));
        assertEquals(12, order.value(1));
        assertEquals(Long.MAX_VALUE, order.key(2));
        assertEquals(14, order.value(2));
        assertEquals(Long.MIN_VALUE, order.key(3));
        assertEquals(13, order.value(3));
        assertEquals(-1L, order.key(4));
        assertEquals(11, order.value(4));
        assertEquals(7L, order.key(5));
        assertThrows(IllegalStateException.class, () -> order.longKey(0));
    }

    @Test
    void exactNullDescriptorOmitsPerEntryValues() {
        final SenkuFlushOrder<Long, NullValue> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorNull(), 2);
        order.set(0, 2L, NullValue.TOMBSTONE);
        order.set(1, 1L, NULL);

        order.sort(0, 2);

        assertEquals(1L, order.longKey(0));
        assertEquals(NULL, order.value(0));
        assertEquals(2L, order.longKey(1));
        assertEquals(NULL, order.value(1));
    }

    private static final class UnsignedLongDescriptor
            extends TypeDescriptorLong {

        @Override
        public Comparator<Long> getComparator() {
            return Long::compareUnsigned;
        }
    }
}
