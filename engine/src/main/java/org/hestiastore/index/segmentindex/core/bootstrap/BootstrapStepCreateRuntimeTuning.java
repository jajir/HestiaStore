package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningConfigurationMapper;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningServiceImpl;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;

/**
 * Creates runtime tuning controls and their persistence callback.
 */
final class BootstrapStepCreateRuntimeTuning<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier =
                new SegmentRuntimeLimitApplier<>(state.getSegmentRegistry(),
                        state.getSegmentRegistry().runtime(),
                        state.getChunkStoreCache());
        state.setRuntimeTuning(newRuntimeTuning(request, state,
                state.getRuntimeTopologyRuntime(), runtimeLimitApplier));
    }

    private RuntimeTuning newRuntimeTuning(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        final Directory directory = request.getDirectory();
        final EffectiveIndexConfiguration<K, V> configuration =
                state.getConfiguration();
        return new RuntimeTuningServiceImpl(state.getRuntimeTuningState(),
                runtimeLimitApplier::apply,
                topologyRuntime::requestFullSplitScan,
                snapshot -> persistRuntimeTuning(directory, configuration,
                        snapshot));
    }

    private void persistRuntimeTuning(final Directory directory,
            final EffectiveIndexConfiguration<K, V> configuration,
            final RuntimeTuningSnapshot snapshot) {
        new IndexConfigurationStorage<K, V>(directory)
                .save(RuntimeTuningConfigurationMapper.apply(configuration,
                        snapshot));
    }
}
