package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.closeRuntimePreparationResources;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configurationWithWal;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BootstrapStepOpenRuntimeWalTest {

    private ExecutorRegistry executorRegistry;
    private SegmentIndexBootstrapState<Integer, String> state;
    private BootstrapStepOpenRuntimeWal<Integer, String> step;
    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepOpenRuntimeWal<>();
        directory = new MemDirectory();
    }

    @AfterEach
    void tearDown() {
        if (state != null) {
            closeRuntimePreparationResources(state);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void apply_populatesWalRuntimeInState() {
        prepareState("bootstrap-step-wal");

        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);

        assertNotNull(state.getRuntimeWalRuntime());
    }

    @Test
    void apply_disabledWalDoesNotOpenWalDirectory() {
        prepareStateWithoutWal("bootstrap-step-wal-disabled");

        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);

        assertFalse(state.hasRuntimeWalRuntime());
        assertFalse(directory.isFileExists(WalRuntime.WAL_DIRECTORY));
    }

    @Test
    void closeResource_closesWalOnRollback() {
        prepareState("bootstrap-step-wal-rollback");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);

        step.closeResource();

        assertThrows(RuntimeException.class,
                () -> state.getRuntimeWalRuntime().appendPut(1, "one"));
    }

    @Test
    void closeResource_skipsCleanupAfterRuntimeWasCreated() {
        prepareState("bootstrap-step-wal-runtime-created");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);
        state.markIndexRuntimeCreated();

        step.closeResource();

        assertDoesNotThrow(
                () -> state.getRuntimeWalRuntime().appendPut(1, "one"));
    }

    private void prepareState(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(configurationWithWal(indexName));
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
    }

    private void prepareStateWithoutWal(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(configuration(indexName));
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
    }
}
