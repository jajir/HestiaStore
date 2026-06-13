package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.teardown.SegmentIndexTeardownPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
final class IndexCloseCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(IndexCloseCoordinator.class);

    private final String indexName;
    private final SegmentIndexStateMachine stateMachine;
    private final SegmentIndexOperationGate operationGate;
    private final IndexOperationStatsRecorder operationStatsRecorder;
    private final IndexRuntimeCloseResources<K, V> closeResources;
    private final ExecutorRegistry executorRegistry;
    private final IndexDirectoryLock directoryLock;

    IndexCloseCoordinator(final String indexName,
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexOperationGate operationGate,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final IndexRuntimeCloseResources<K, V> closeResources,
            final ExecutorRegistry executorRegistry,
            final IndexDirectoryLock directoryLock) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
        this.operationStatsRecorder = Vldtn.requireNonNull(
                operationStatsRecorder, "operationStatsRecorder");
        this.closeResources = Vldtn.requireNonNull(closeResources,
                "closeResources");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.directoryLock = Vldtn.requireNonNull(directoryLock,
                "directoryLock");
    }

    void close() {
        LOGGER.debug("Closing index '{}'.", indexName);
        try {
            stateMachine.beginClose();
            teardownPipeline().run(this);
            LOGGER.debug("Index '{}' closed.", indexName);
        } catch (final RuntimeException e) {
            stateMachine.markRuntimeFailure(e);
            throw e;
        }
    }

    private SegmentIndexTeardownPipeline<IndexCloseCoordinator<K, V>> teardownPipeline() {
        return SegmentIndexTeardownPipeline
                .of(IndexCloseTeardownSteps.<K, V>closeSteps());
    }

    SegmentIndexOperationGate operationGate() {
        return operationGate;
    }

    IndexRuntimeCloseResources<K, V> closeResources() {
        return closeResources;
    }

    IndexOperationStatsRecorder operationStatsRecorder() {
        return operationStatsRecorder;
    }

    ExecutorRegistry executorRegistry() {
        return executorRegistry;
    }

    SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }

    IndexDirectoryLock directoryLock() {
        return directoryLock;
    }
}
