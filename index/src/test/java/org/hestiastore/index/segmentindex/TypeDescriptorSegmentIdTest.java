package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class TypeDescriptorSegmentIdTest {

    @Test
    void comparatorSortsDescendingById() {
        final TypeDescriptorSegmentId descriptor = new TypeDescriptorSegmentId();

        final SegmentId lower = SegmentId.of(1);
        final SegmentId higher = SegmentId.of(2);

        assertTrue(descriptor.getComparator().compare(lower, higher) > 0);
    }

    @Test
    void convertorsRoundTripSegmentId() {
        final TypeDescriptorSegmentId descriptor = new TypeDescriptorSegmentId();
        final SegmentId segmentId = SegmentId.of(5);

        final byte[] bytes = descriptor.getConvertorToBytes()
                .toBytes(segmentId);
        final SegmentId restored = descriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(segmentId, restored);
    }

    @Test
    void tombstoneUsesInvalidSegmentIdValue() {
        final TypeDescriptorSegmentId descriptor = new TypeDescriptorSegmentId();

        assertThrows(IllegalArgumentException.class,
                descriptor::getTombstone,
                String.format("SegmentId tombstone %s is invalid",
                        TypeDescriptorInteger.TOMBSTONE_VALUE));
    }
}
