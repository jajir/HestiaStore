package org.hestiastore.index.segment;

/**
 * Class test invalid parameters of segment.
 */

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentBuilderTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);
    private static final Directory DIRECTORY = new MemDirectory();
    private static final TypeDescriptor<String> VALUE_TYPE_DESCRIPTOR = new TypeDescriptorString();
    private static final TypeDescriptor<Integer> KEY_TYPE_DESCRIPTOR = new TypeDescriptorInteger();

    @Test
    void test_directory_is_missing() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                // .withDirectory(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Directory can't be null", e.getMessage());
    }

    @Test
    void test_keyTypeDescriptor_is_missing() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectory(DIRECTORY)//
                .withId(SEGMENT_ID)//
                // .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("KeyTypeDescriptor can't be null", e.getMessage());
    }

    @Test
    void test_valueTypeDescriptor_is_missing() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectory(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                // .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("ValueTypeDescriptor can't be null", e.getMessage());
    }

    @Test
    void test_withMaxNumberOfKeysInSegmentCache_is_1() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectory(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(1)//
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals(
                "maxNumberOfKeysInSegmentCache is '1' but must be higher than '1'",
                e.getMessage());
    }

    @Test
    void test_MaxNumberOfKeysInSegmentCacheDuringFlushing_is_too_low() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectory(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(10)
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals(
                "maxNumberOfKeysInSegmentCacheDuringFlushing must be higher than maxNumberOfKeysInSegmentCache",
                e.getMessage());
    }
}
