package org.hestiastore.index.segmentindex.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.datatype.TestEncoding;
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

        final byte[] bytes = TestEncoding.toByteArray(descriptor.getTypeEncoder(),
                segmentId);
        final SegmentId restored = descriptor.getTypeDecoder()
                .decode(bytes);

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

    @Test
    void encoderLengthAndDestinationValidation() {
        final TypeDescriptorSegmentId descriptor = new TypeDescriptorSegmentId();
        final TypeEncoder<SegmentId> encoder = descriptor.getTypeEncoder();
        final SegmentId segmentId = SegmentId.of(9);

        assertEquals(Integer.BYTES, encoder.bytesLength(segmentId));
        final byte[] destination = new byte[Integer.BYTES];
        assertEquals(Integer.BYTES, encoder.toBytes(segmentId, destination));
        assertThrows(IllegalArgumentException.class,
                () -> encoder.toBytes(segmentId, new byte[Integer.BYTES - 1]));
    }
}
