package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntime;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeServices;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionInfrastructure;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.junit.jupiter.api.Test;

class BootstrapStepCreateIndexTest {

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCreateIndex<Integer, String>(null));
    }

    @Test
    void apply_createsIndexHandleThroughSessionFactory() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("bootstrap-step-create-index");
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(configuration);
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexSessionResources<Integer, String> sessionResources =
                initializedSessionResources(directory, keyDescriptor);
        final BootstrapStepCreateIndex<Integer, String> step =
                new BootstrapStepCreateIndex<>(sessionResources);
        state.setKeyTypeDescriptor(keyDescriptor);

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE),
                state));

        assertNotNull(state.getIndexHandle());
    }

    private <K, V> SegmentIndexSessionResources<K, V> initializedSessionResources(
            final Directory directory,
            final TypeDescriptor<K> keyDescriptor) {
        final SegmentIndexSessionResources<K, V> sessionResources =
                new SegmentIndexSessionResources<>();
        sessionResources.acquireDirectoryLock(directory);
        sessionResources.setSessionInfrastructure(
                SegmentIndexSessionInfrastructure.create());
        sessionResources.setRuntime(newRuntime(keyDescriptor),
                mock(ExecutorRegistry.class));
        return sessionResources;
    }

    @SuppressWarnings("unchecked")
    private <K, V> SegmentIndexRuntime<K, V> newRuntime(
            final TypeDescriptor<K> keyDescriptor) {
        return new SegmentIndexRuntime<>(keyDescriptor,
                mock(StorageService.class), () -> {
                }, mock(SegmentTopologyRuntimeAccess.class),
                new SegmentIndexRuntimeServices<>(
                        mock(SegmentIndexOperationAccess.class),
                        mock(MaintenanceService.class),
                        mock(IndexRuntimeMonitoring.class),
                        mock(RuntimeTuning.class)));
    }
}
