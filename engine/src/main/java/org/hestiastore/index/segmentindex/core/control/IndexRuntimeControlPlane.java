package org.hestiastore.index.segmentindex.core.control;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Implements runtime monitoring and runtime patching over one index instance.
 */
public final class IndexRuntimeControlPlane implements IndexControlPlane {

    private final IndexConfiguration<?, ?> conf;
    private final RuntimeConfigPatchValidator patchValidator;
    private final RuntimeConfigPatchApplier patchApplier;
    private final IndexRuntimeView runtime;
    private final IndexConfigurationManagement configuration;

    public IndexRuntimeControlPlane(final IndexConfiguration<?, ?> conf,
            final RuntimeTuningState runtimeTuningState,
            final Supplier<SegmentIndexState> stateSupplier,
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        final RuntimeTuningState validatedRuntimeTuningState = Vldtn
                .requireNonNull(runtimeTuningState, "runtimeTuningState");
        final Supplier<SegmentIndexState> validatedStateSupplier = Vldtn
                .requireNonNull(stateSupplier, "stateSupplier");
        final Supplier<SegmentIndexMetricsSnapshot> validatedMetricsSnapshotSupplier =
                Vldtn.requireNonNull(metricsSnapshotSupplier,
                        "metricsSnapshotSupplier");
        this.patchValidator = new RuntimeConfigPatchValidator(
                validatedRuntimeTuningState);
        this.patchApplier = new RuntimeConfigPatchApplier(this.patchValidator,
                validatedRuntimeTuningState,
                Vldtn.requireNonNull(effectiveLimitsApplier,
                        "effectiveLimitsApplier"),
                Vldtn.requireNonNull(splitThresholdChangedListener,
                        "splitThresholdChangedListener"));
        this.runtime = new IndexRuntimeSnapshotView(conf,
                validatedStateSupplier, validatedMetricsSnapshotSupplier);
        this.configuration = new IndexRuntimeConfigurationManagement(
                validatedRuntimeTuningState, this::validate, this::apply);
    }

    @Override
    public String indexName() {
        return conf.getIndexName();
    }

    @Override
    public IndexRuntimeView runtime() {
        return runtime;
    }

    @Override
    public IndexConfigurationManagement configuration() {
        return configuration;
    }

    RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
        return patchValidator.validate(patch);
    }

    RuntimePatchResult apply(final RuntimeConfigPatch patch) {
        return patchApplier.apply(patch);
    }

}
