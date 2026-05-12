package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class SegmentIndexBootstrapPipelineTest {

    private static final String MDC_INDEX_NAME_KEY = "index.name";

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void run_closesAppliedStepsInReverseOrderWhenStepFails() {
        final List<String> calls = new ArrayList<>();
        final RuntimeException failure = new IllegalStateException("boom");
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(
                        List.of(step("one", calls), step("two", calls),
                                step("three", calls, failure)));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> pipeline.run(request(), new SegmentIndexBootstrapState<>()));

        assertSame(failure, thrown);
        assertEquals(List.of("apply one", "apply two", "apply three",
                "close three", "close two", "close one"), calls);
    }

    @Test
    void run_closesFailingStepWhenApplyThrowsAfterPartialResourceCreation() {
        final List<String> calls = new ArrayList<>();
        final RuntimeException failure = new IllegalStateException("boom");
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(
                        List.of(step("resource", calls, failure)));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> pipeline.run(request(), new SegmentIndexBootstrapState<>()));

        assertSame(failure, thrown);
        assertEquals(List.of("apply resource", "close resource"), calls);
    }

    @Test
    void run_attachesCleanupFailuresToOriginalFailure() {
        final List<String> calls = new ArrayList<>();
        final RuntimeException failure = new IllegalStateException("boom");
        final RuntimeException cleanupFailure =
                new IllegalStateException("cleanup");
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(
                        List.of(step("first", calls, null, cleanupFailure),
                                step("second", calls, failure)));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> pipeline.run(request(), new SegmentIndexBootstrapState<>()));

        assertSame(failure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(cleanupFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void run_executesStepsAndCleanupInsideMdcScopeAfterRunnerExists() {
        final List<String> calls = new ArrayList<>();
        final RuntimeException failure = new IllegalStateException("boom");
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(List.of(
                        new ScopeRunnerStep("bootstrap-pipeline-test", calls),
                        new MdcRecordingStep("resource", calls, failure)));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> pipeline.run(request(), new SegmentIndexBootstrapState<>()));

        assertSame(failure, thrown);
        assertEquals(List.of("apply resource:bootstrap-pipeline-test",
                "close resource:bootstrap-pipeline-test",
                "close scope:bootstrap-pipeline-test"), calls);
        assertNull(MDC.get(MDC_INDEX_NAME_KEY));
    }

    @Test
    void run_passesRequestAndStateToSteps() {
        final SegmentIndexBootstrapRequest<Integer, String> request = request();
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        final List<Boolean> calls = new ArrayList<>();
        final SegmentIndex<Integer, String> index = mockIndex();
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(List.of(
                        new RequestStateAssertingStep(request, state, calls),
                        new SetIndexStep(index)));

        pipeline.run(request, state);

        assertEquals(List.of(Boolean.TRUE), calls);
    }

    @Test
    void run_returnsCreatedResultForCreateMode() {
        final SegmentIndex<Integer, String> index = mockIndex();
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(
                        List.of(new SetIndexStep(index)));

        final SegmentIndexBootstrapResult<Integer, String> result =
                pipeline.run(request(SegmentIndexBootstrapMode.CREATE), state);

        assertEquals(SegmentIndexBootstrapStatus.CREATED, result.status());
        assertSame(index, result.requireIndex());
    }

    @Test
    void run_returnsOpenedResultForOpenMode() {
        final SegmentIndex<Integer, String> index = mockIndex();
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(
                        List.of(new SetIndexStep(index)));

        final SegmentIndexBootstrapResult<Integer, String> result =
                pipeline.run(request(SegmentIndexBootstrapMode.OPEN), state);

        assertEquals(SegmentIndexBootstrapStatus.OPENED, result.status());
        assertSame(index, result.requireIndex());
    }

    @Test
    void run_returnsStateResultAndStopsRemainingSteps() {
        final List<String> calls = new ArrayList<>();
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(List.of(
                        new SetResultStep(calls), step("unused", calls)));

        final SegmentIndexBootstrapResult<Integer, String> result = pipeline.run(
                request(SegmentIndexBootstrapMode.TRY_OPEN),
                new SegmentIndexBootstrapState<>());

        assertEquals(SegmentIndexBootstrapStatus.NOT_FOUND, result.status());
        assertTrue(result.index().isEmpty());
        assertEquals(List.of("set result"), calls);
    }

    @Test
    void run_closesAppliedStepsBeforeReturningNoIndexResult() {
        final List<String> calls = new ArrayList<>();
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(List.of(
                        step("resource", calls), new SetResultStep(calls),
                        step("unused", calls)));

        final SegmentIndexBootstrapResult<Integer, String> result = pipeline.run(
                request(SegmentIndexBootstrapMode.TRY_OPEN),
                new SegmentIndexBootstrapState<>());

        assertEquals(SegmentIndexBootstrapStatus.NOT_FOUND, result.status());
        assertTrue(result.index().isEmpty());
        assertEquals(List.of("apply resource", "set result", "close resource"),
                calls);
    }

    @Test
    void run_throwsCleanupFailureBeforeReturningNoIndexResult() {
        final List<String> calls = new ArrayList<>();
        final RuntimeException cleanupFailure =
                new IllegalStateException("cleanup");
        final SegmentIndexBootstrapPipeline<Integer, String> pipeline =
                new SegmentIndexBootstrapPipeline<>(List.of(
                        step("resource", calls, null, cleanupFailure),
                        new SetResultStep(calls)));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> pipeline.run(request(SegmentIndexBootstrapMode.TRY_OPEN),
                        new SegmentIndexBootstrapState<>()));

        assertSame(cleanupFailure, thrown);
        assertEquals(List.of("apply resource", "set result", "close resource"),
                calls);
    }

    private SegmentIndexBootstrapRequest<Integer, String> request() {
        return request(SegmentIndexBootstrapMode.CREATE);
    }

    private SegmentIndexBootstrapRequest<Integer, String> request(
            final SegmentIndexBootstrapMode mode) {
        return new SegmentIndexBootstrapRequest<>(new MemDirectory(),
                buildConf(), null, mode);
    }

    private static RecordingStep step(final String name,
            final List<String> calls) {
        return step(name, calls, null);
    }

    private static RecordingStep step(final String name,
            final List<String> calls,
            final RuntimeException applyFailure) {
        return step(name, calls, applyFailure, null);
    }

    private static RecordingStep step(final String name,
            final List<String> calls,
            final RuntimeException applyFailure,
            final RuntimeException closeFailure) {
        return new RecordingStep(name, calls, applyFailure, closeFailure);
    }

    @SuppressWarnings("unchecked")
    private static SegmentIndex<Integer, String> mockIndex() {
        return mock(SegmentIndex.class);
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(
                        new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(
                        new TypeDescriptorShortString()))
                .identity(identity -> identity.name(
                        "segment-index-bootstrap-pipeline-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static final class RecordingStep
            extends SegmentIndexBootstrapStep<Integer, String> {

        private final String name;
        private final List<String> calls;
        private final RuntimeException applyFailure;
        private final RuntimeException closeFailure;

        private RecordingStep(final String name, final List<String> calls,
                final RuntimeException applyFailure,
                final RuntimeException closeFailure) {
            this.name = name;
            this.calls = calls;
            this.applyFailure = applyFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        void apply(
                final SegmentIndexBootstrapRequest<Integer, String> request,
                final SegmentIndexBootstrapState<Integer, String> state) {
            calls.add("apply " + name);
            if (applyFailure != null) {
                throw applyFailure;
            }
        }

        @Override
        void closeResource() {
            calls.add("close " + name);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class ScopeRunnerStep
            extends SegmentIndexBootstrapStep<Integer, String> {

        private final String indexName;
        private final List<String> calls;

        private ScopeRunnerStep(final String indexName,
                final List<String> calls) {
            this.indexName = indexName;
            this.calls = calls;
        }

        @Override
        void apply(
                final SegmentIndexBootstrapRequest<Integer, String> request,
                final SegmentIndexBootstrapState<Integer, String> state) {
            state.setIndexMdcScopeRunner(new IndexMdcScopeRunner(indexName));
        }

        @Override
        void closeResource() {
            calls.add("close scope:" + MDC.get(MDC_INDEX_NAME_KEY));
        }
    }

    private static final class MdcRecordingStep
            extends SegmentIndexBootstrapStep<Integer, String> {

        private final String name;
        private final List<String> calls;
        private final RuntimeException applyFailure;

        private MdcRecordingStep(final String name, final List<String> calls,
                final RuntimeException applyFailure) {
            this.name = name;
            this.calls = calls;
            this.applyFailure = applyFailure;
        }

        @Override
        void apply(
                final SegmentIndexBootstrapRequest<Integer, String> request,
                final SegmentIndexBootstrapState<Integer, String> state) {
            calls.add("apply " + name + ":" + MDC.get(MDC_INDEX_NAME_KEY));
            if (applyFailure != null) {
                throw applyFailure;
            }
        }

        @Override
        void closeResource() {
            calls.add("close " + name + ":" + MDC.get(MDC_INDEX_NAME_KEY));
        }
    }

    private static final class SetIndexStep
            extends SegmentIndexBootstrapStep<Integer, String> {

        private final SegmentIndex<Integer, String> index;

        private SetIndexStep(final SegmentIndex<Integer, String> index) {
            this.index = index;
        }

        @Override
        void apply(
                final SegmentIndexBootstrapRequest<Integer, String> request,
                final SegmentIndexBootstrapState<Integer, String> state) {
            state.setIndex(index);
        }
    }

    private static final class SetResultStep
            extends SegmentIndexBootstrapStep<Integer, String> {

        private final List<String> calls;

        private SetResultStep(final List<String> calls) {
            this.calls = calls;
        }

        @Override
        void apply(
                final SegmentIndexBootstrapRequest<Integer, String> request,
                final SegmentIndexBootstrapState<Integer, String> state) {
            calls.add("set result");
            state.setResult(SegmentIndexBootstrapResult.notFound());
        }
    }

    private static final class RequestStateAssertingStep
            extends SegmentIndexBootstrapStep<Integer, String> {

        private final SegmentIndexBootstrapRequest<Integer, String> expectedRequest;
        private final SegmentIndexBootstrapState<Integer, String> expectedState;
        private final List<Boolean> calls;

        private RequestStateAssertingStep(
                final SegmentIndexBootstrapRequest<Integer, String> expectedRequest,
                final SegmentIndexBootstrapState<Integer, String> expectedState,
                final List<Boolean> calls) {
            this.expectedRequest = expectedRequest;
            this.expectedState = expectedState;
            this.calls = calls;
        }

        @Override
        void apply(
                final SegmentIndexBootstrapRequest<Integer, String> request,
                final SegmentIndexBootstrapState<Integer, String> state) {
            calls.add(Boolean.valueOf(request == expectedRequest
                    && state == expectedState));
            assertEquals(SegmentIndexBootstrapMode.CREATE, request.getMode());
        }
    }
}
