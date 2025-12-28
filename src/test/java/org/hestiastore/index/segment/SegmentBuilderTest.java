package org.hestiastore.index.segment;

/**
 * Class test invalid parameters of segment.
 */

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

/**
 * Class test invalid parameters of segment. There are two kind of tests 1)
 * verify that with methods are correctly validating input and second that
 * configuration is correctly used.
 */
class SegmentBuilderTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);
    private static final Directory DIRECTORY = new MemDirectory();
    private static final TypeDescriptor<String> VALUE_TYPE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final TypeDescriptor<Integer> KEY_TYPE_DESCRIPTOR = new TypeDescriptorInteger();

    @Test
    void test_directory_is_missing() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                // .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
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
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
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
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
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
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
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
    void test_withMaxNumberOfKeysInSegmentCacheDuringFlushing_is_too_low() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
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

    @Test
    void test_encodingChunkFilters_are_missing() {
        final SegmentConf segmentConf = mock(SegmentConf.class);
        when(segmentConf.getEncodingChunkFilters()).thenReturn(null);

        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withSegmentConf(segmentConf)//
        ;

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Property 'encodingChunkFilters' must not be null.",
                e.getMessage());
    }

    @Test
    void test_decodingChunkFilters_are_missing() {
        final SegmentConf segmentConf = mock(SegmentConf.class);
        when(segmentConf.getEncodingChunkFilters())
                .thenReturn(List.of(new ChunkFilterDoNothing()));
        when(segmentConf.getDecodingChunkFilters()).thenReturn(List.of());

        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                // .withEncodingChunkFilters(List.of(new
                // ChunkFilterDoNothing()))
                .withSegmentConf(segmentConf)//
        ;

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Property 'decodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_withEncodingChunkFilters_null() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withEncodingChunkFilters(null));

        assertEquals("Property 'encodingChunkFilters' must not be null.",
                e.getMessage());
    }

    @Test
    void test_withEncodingChunkFilters_empty() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withEncodingChunkFilters(List.of()));

        assertEquals("Property 'encodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_withDecodingChunkFilters_null() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withDecodingChunkFilters(null));

        assertEquals("Property 'decodingChunkFilters' must not be null.",
                e.getMessage());
    }

    @Test
    void test_withDecodingChunkFilters_empty() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withDecodingChunkFilters(List.of()));

        assertEquals("Property 'decodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_build_withProvidedChunkFilters() {
        final SegmentImpl<Integer, String> segment = Segment
                .<Integer, String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build();

        assertNotNull(segment);
        assertEquals(1,
                segment.getSegmentConf().getEncodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class, segment.getSegmentConf()
                .getEncodingChunkFilters().get(0).getClass());
        assertEquals(1,
                segment.getSegmentConf().getDecodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class, segment.getSegmentConf()
                .getDecodingChunkFilters().get(0).getClass());
        segment.close();
    }
}
