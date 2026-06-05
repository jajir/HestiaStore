package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Creates one-use step lists for the full segment-index bootstrap flow.
 */
final class SegmentIndexBootstrapSteps {

    private SegmentIndexBootstrapSteps() {
    }

    static <K, V> List<SegmentIndexBootstrapStep<K, V>> startingSteps() {
        final SegmentIndexSessionResources<K, V> sessionResources = new SegmentIndexSessionResources<>();
        final List<SegmentIndexBootstrapStep<K, V>> steps = new ArrayList<>();
        steps.add(checkExistingConfiguration());
        steps.add(new BootstrapStepAcquireDirectoryLock<>(sessionResources));
        steps.add(rejectExistingConfigurationForCreate());
        steps.add(resolveConfiguration());
        steps.add(createMdcCallWrapper());
        steps.add(resolveTypeDescriptors());
        steps.add(writeConfiguration());
        steps.add(createExecutorRegistry());
        steps.add(new BootstrapStepCreateSessionInfrastructure<>(
                sessionResources));
        steps.add(new BootstrapStepOpenCoreStorage<>());
        steps.add(new BootstrapStepCreateRuntimeTopology<>(sessionResources));
        steps.add(new BootstrapStepOpenRuntimeWal<>());
        steps.add(new BootstrapStepCreateRuntimeServices<>(sessionResources));
        steps.add(new BootstrapStepCreateRuntime<>(sessionResources));
        steps.add(new BootstrapStepCreateIndex<>(sessionResources));
        steps.add(new BootstrapStepCompleteStartup<>(sessionResources));
        steps.add(applyContextLogging());
        steps.add(wrapResourceClosing());
        return List.copyOf(steps);
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> resolveConfiguration() {
        return new BootstrapStepResolveConfiguration<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> checkExistingConfiguration() {
        return new BootstrapStepCheckExistingConfiguration<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> rejectExistingConfigurationForCreate() {
        return new BootstrapStepRejectExistingConfigurationForCreate<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> createMdcCallWrapper() {
        return new BootstrapStepCreateMdcCallWrapper<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> resolveTypeDescriptors() {
        return new BootstrapStepResolveTypeDescriptors<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> writeConfiguration() {
        return new BootstrapStepWriteConfiguration<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> createExecutorRegistry() {
        return new BootstrapStepCreateExecutorRegistry<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> applyContextLogging() {
        return new BootstrapStepApplyContextLogging<>();
    }

    static <K, V> SegmentIndexBootstrapStep<K, V> wrapResourceClosing() {
        return new BootstrapStepWrapResourceClosing<>();
    }
}
