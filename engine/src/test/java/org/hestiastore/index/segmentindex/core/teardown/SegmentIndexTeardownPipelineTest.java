package org.hestiastore.index.segmentindex.core.teardown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentIndexTeardownPipelineTest {

    @Test
    void run_executesAllStepsWhenEarlierStepsFail() {
        final List<String> calls = new ArrayList<>();
        final RuntimeException firstFailure =
                new IllegalStateException("first");
        final RuntimeException secondFailure =
                new IllegalStateException("second");
        final SegmentIndexTeardownPipeline<String> pipeline =
                new SegmentIndexTeardownPipeline<>(List.of(
                        closeAction("one", calls, firstFailure),
                        closeAction("two", calls, null),
                        closeAction("three", calls, secondFailure)));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> pipeline.run("context"));

        assertSame(firstFailure, thrown);
        assertEquals(List.of("one", "two", "three"), calls);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(secondFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void run_completesWhenAllStepsSucceed() {
        final List<String> calls = new ArrayList<>();
        final SegmentIndexTeardownPipeline<String> pipeline =
                new SegmentIndexTeardownPipeline<>(List.of(
                        closeAction("one", calls, null),
                        closeAction("two", calls, null)));

        pipeline.run("context");

        assertEquals(List.of("one", "two"), calls);
    }

    @Test
    void run_rejectsNullContext() {
        final SegmentIndexTeardownPipeline<String> pipeline =
                SegmentIndexTeardownPipeline.of(List.of());

        assertThrows(IllegalArgumentException.class, () -> pipeline.run(null));
    }

    private static SegmentIndexTeardownStep<String> closeAction(
            final String name,
            final List<String> calls,
            final RuntimeException failure) {
        return context -> {
            calls.add(name);
            if (failure != null) {
                throw failure;
            }
        };
    }
}
