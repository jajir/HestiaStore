package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ByteToolTest {

    @Test
    void test_count_matching_prefix_bytes() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3, 4 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 1, 2, 9 });

        assertEquals(2, ByteTool.countMatchingPrefixBytes(first, second));
    }

    @Test
    void test_get_remaining_bytes_after_index() {
        final ByteSequence data = ByteSequences.wrap(new byte[] { 5, 6, 7 });

        assertSame(data, ByteTool.getRemainingBytesAfterIndex(0, data));
        assertSame(ByteSequence.EMPTY, ByteTool.getRemainingBytesAfterIndex(3, data));
        assertEquals(2, ByteTool.getRemainingBytesAfterIndex(1, data).length());
        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.getRemainingBytesAfterIndex(4, data));
    }

    @Test
    void test_concatenate() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 3 });

        assertEquals(3, ByteTool.concatenate(first, second).length());
    }
}
