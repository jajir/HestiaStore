package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SegmentSplitApiTest {

    private static final SegmentId NEW_SEGMENT_ID = SegmentId.of(42);

    static Stream<Arguments> segmentFactories() {
        return Stream.of(
                Arguments.of("SegmentImpl",
                        (Supplier<Segment<Integer, String>>) SegmentSplitApiTest::newSegment),
                Arguments.of("SegmentSynchronizationAdapter",
                        (Supplier<Segment<Integer, String>>) () -> new SegmentSynchronizationAdapter<>(
                                newSegment())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("segmentFactories")
    void split_rejects_null_segment_id(final String label,
            final Supplier<Segment<Integer, String>> factory) {
        try (Segment<Integer, String> segment = factory.get()) {
            final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                    .fromPolicy(segment.getSegmentSplitterPolicy());
            final IllegalArgumentException err = assertThrows(
                    IllegalArgumentException.class,
                    () -> segment.split(null, plan));
            assertEquals("Property 'segmentId' must not be null.",
                    err.getMessage());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("segmentFactories")
    void split_rejects_null_plan(final String label,
            final Supplier<Segment<Integer, String>> factory) {
        try (Segment<Integer, String> segment = factory.get()) {
            final IllegalArgumentException err = assertThrows(
                    IllegalArgumentException.class,
                    () -> segment.split(NEW_SEGMENT_ID, null));
            assertEquals("Property 'plan' must not be null.", err.getMessage());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("segmentFactories")
    void split_fails_when_plan_not_feasible(final String label,
            final Supplier<Segment<Integer, String>> factory) {
        try (Segment<Integer, String> segment = factory.get()) {
            final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                    .fromPolicy(segment.getSegmentSplitterPolicy());
            final IllegalStateException err = assertThrows(
                    IllegalStateException.class,
                    () -> segment.split(NEW_SEGMENT_ID, plan));
            assertEquals("Splitting failed. Number of keys is too low.",
                    err.getMessage());
        }
    }

    @Test
    void split_waits_for_iterator_close_in_synchronized_adapter()
            throws Exception {
        try (SegmentSynchronizationAdapter<Integer, String> segment = new SegmentSynchronizationAdapter<>(
                newSegment())) {
            final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                    .fromPolicy(segment.getSegmentSplitterPolicy());
            final EntryIterator<Integer, String> iterator = segment
                    .openIterator();
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                final Future<?> future = executor
                        .submit(() -> segment.split(NEW_SEGMENT_ID, plan));
                assertThrows(TimeoutException.class,
                        () -> future.get(200, TimeUnit.MILLISECONDS));
                iterator.close();
                final ExecutionException err = assertThrows(
                        ExecutionException.class,
                        () -> future.get(2, TimeUnit.SECONDS));
                assertTrue(err.getCause() instanceof IllegalStateException);
            } finally {
                if (!iterator.wasClosed()) {
                    iterator.close();
                }
                executor.shutdownNow();
            }
        }
    }

    private static Segment<Integer, String> newSegment() {
        final Directory directory = new MemDirectory();
        return Segment.<Integer, String>builder()//
                .withDirectory(directory)//
                .withId(SegmentId.of(1))//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
