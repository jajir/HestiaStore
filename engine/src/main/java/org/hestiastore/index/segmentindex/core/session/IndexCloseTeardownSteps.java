package org.hestiastore.index.segmentindex.core.session;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.F;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.teardown.SegmentIndexTeardownStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates one-use step lists for the full segment-index close flow.
 */
final class IndexCloseTeardownSteps {

    private IndexCloseTeardownSteps() {
    }

    static <K, V> List<SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>>> closeSteps() {
        final List<SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>>> steps =
                new ArrayList<>();
        steps.add(new AwaitForegroundOperations<>());
        steps.add(new CloseSplitRuntime<>());
        steps.add(new SealAsyncMaintenanceAndWait<>());
        steps.add(new FlushAndWait<>());
        steps.add(new CloseSegmentRegistry<>());
        steps.add(new CloseKeyToSegmentMapIfOpen<>());
        steps.add(new LogOperationCounts<>());
        steps.add(new ReleaseWalRuntime<>());
        steps.add(new CloseExecutorRegistry<>());
        steps.add(new FinishClosedState<>());
        steps.add(new ReleaseDirectoryLock<>());
        return List.copyOf(steps);
    }

    private static final class AwaitForegroundOperations<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.operationGate().awaitOperationDrain();
        }
    }

    private static final class CloseSplitRuntime<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.runtime().closeSplitRuntime();
        }
    }

    private static final class SealAsyncMaintenanceAndWait<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.runtime().sealAsyncMaintenanceAndWait();
        }
    }

    private static final class FlushAndWait<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.runtime().flushAndWait();
        }
    }

    private static final class CloseSegmentRegistry<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.runtime().closeSegmentRegistry();
        }
    }

    private static final class CloseKeyToSegmentMapIfOpen<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.runtime().closeKeyToSegmentMapIfOpen();
        }
    }

    private static final class LogOperationCounts<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        private static final Logger LOGGER = LoggerFactory
                .getLogger(LogOperationCounts.class);

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            final IndexOperationStatsRecorder recorder =
                    context.operationStatsRecorder();
            if (!LOGGER.isDebugEnabled()) {
                return;
            }
            final IndexOperationStats stats = recorder.statsSnapshot();
            LOGGER.debug(String.format(
                    "Index is closing, where was %s gets, %s puts and %s deletes.",
                    F.fmt(stats.getGetCount()), F.fmt(stats.getPutCount()),
                    F.fmt(stats.getDeleteCount())));
        }
    }

    private static final class ReleaseWalRuntime<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.runtime().closeWalRuntime();
        }
    }

    private static final class CloseExecutorRegistry<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.executorRegistry().close();
        }
    }

    private static final class FinishClosedState<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.stateMachine().completeClose();
        }
    }

    private static final class ReleaseDirectoryLock<K, V>
            implements SegmentIndexTeardownStep<IndexCloseCoordinator<K, V>> {

        @Override
        public void apply(final IndexCloseCoordinator<K, V> context) {
            context.directoryLock().close();
        }
    }
}
