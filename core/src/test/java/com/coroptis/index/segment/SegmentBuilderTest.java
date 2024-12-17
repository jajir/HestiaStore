package com.coroptis.index.segment;

/**
 * Class test invalid parameters of segment.
 */

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SegmentBuilderTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);
    private final static Directory DIRECTORY = new MemDirectory();
    private static final TypeDescriptor<String> VALUE_TYPE_DESCRIPTOR = new TypeDescriptorString();
    private static final TypeDescriptor<Integer> KEY_TYPE_DESCRIPTOR = new TypeDescriptorInteger();

    @Test
    public void test_directory_is_missing() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Segment.<Integer, String>builder()//
                        // .withDirectory(DIRECTORY)//
                        .withId(SEGMENT_ID)//
                        .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                        .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                        .withMaxNumberOfKeysInSegmentCache(10)//
                        .withBloomFilterIndexSizeInBytes(0)//
                        .build());

        assertEquals("Directory can't be null", e.getMessage());
    }

    @Test
    public void test_keyTypeDescriptor_is_missing() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Segment.<Integer, String>builder()//
                        .withDirectory(DIRECTORY)//
                        .withId(SEGMENT_ID)//
                        // .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                        .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                        .withMaxNumberOfKeysInSegmentCache(10)//
                        .withBloomFilterIndexSizeInBytes(0)//
                        .build());

        assertEquals("KeyTypeDescriptor can't be null", e.getMessage());
    }

    @Test
    public void test_valueTypeDescriptor_is_missing() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Segment.<Integer, String>builder()//
                        .withDirectory(DIRECTORY)//
                        .withId(SEGMENT_ID)//
                        .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                        // .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                        .withMaxNumberOfKeysInSegmentCache(10)//
                        .withBloomFilterIndexSizeInBytes(0)//
                        .build());

        assertEquals("ValueTypeDescriptor can't be null", e.getMessage());
    }

    @Test
    public void test_withMaxNumberOfKeysInSegmentCache_is_1() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Segment.<Integer, String>builder()//
                        .withDirectory(DIRECTORY)//
                        .withId(SEGMENT_ID)//
                        .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                        .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                        .withMaxNumberOfKeysInSegmentCache(1)//
                        .withBloomFilterIndexSizeInBytes(0)//
                        .build());

        assertEquals("maxNumberOfKeysInSegmentCache is '1' but must be higher than '1'",
                e.getMessage());
    }

    @Test
    public void test_MaxNumberOfKeysInSegmentCacheDuringFlushing_is_too_low() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Segment.<Integer, String>builder()//
                        .withDirectory(DIRECTORY)//
                        .withId(SEGMENT_ID)//
                        .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                        .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                        .withMaxNumberOfKeysInSegmentCache(10)//
                        .withMaxNumberOfKeysInSegmentCacheDuringFlushing(10)
                        .withBloomFilterIndexSizeInBytes(0)//
                        .build());

        assertEquals(
                "maxNumberOfKeysInSegmentCacheDuringFlushing must be higher than maxNumberOfKeysInSegmentCache",
                e.getMessage());
    }
}
