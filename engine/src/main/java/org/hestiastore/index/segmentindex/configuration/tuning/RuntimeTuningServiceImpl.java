package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Implements runtime patching over one index instance.
 */
public final class RuntimeTuningServiceImpl
        implements RuntimeTuning {

    private final RuntimeTuningPatchValidator patchValidator;
    private final RuntimeTuningPatchApplier patchApplier;
    private final RuntimeTuningState runtimeTuningState;
    private final Consumer<RuntimeTuningSnapshot> snapshotPersister;

    public RuntimeTuningServiceImpl(final RuntimeTuningState runtimeTuningState,
            final Consumer<RuntimeTuningSnapshot> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this(runtimeTuningState, effectiveLimitsApplier,
                splitThresholdChangedListener,
                RuntimeTuningServiceImpl::unsupportedPersistence);
    }

    public RuntimeTuningServiceImpl(final RuntimeTuningState runtimeTuningState,
            final Consumer<RuntimeTuningSnapshot> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener,
            final Consumer<RuntimeTuningSnapshot> snapshotPersister) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.snapshotPersister = Vldtn.requireNonNull(snapshotPersister,
                "snapshotPersister");
        this.patchValidator = new RuntimeTuningPatchValidator(
                this.runtimeTuningState);
        this.patchApplier = new RuntimeTuningPatchApplier(patchValidator,
                this.runtimeTuningState,
                Vldtn.requireNonNull(effectiveLimitsApplier,
                        "effectiveLimitsApplier"),
                Vldtn.requireNonNull(splitThresholdChangedListener,
                        "splitThresholdChangedListener"));
    }

    @Override
    public RuntimeTuningSnapshot current() {
        return runtimeTuningState.snapshotCurrent();
    }

    @Override
    public RuntimeTuningSnapshot original() {
        return runtimeTuningState.snapshotOriginal();
    }

    @Override
    public RuntimeTuningValidation validate(final RuntimeTuningPatch patch) {
        return patchValidator.validate(patch);
    }

    @Override
    public RuntimeTuningResult apply(final RuntimeTuningPatch patch) {
        return patchApplier.apply(patch);
    }

    @Override
    public RuntimeTuningSnapshot persistCurrent() {
        final RuntimeTuningSnapshot snapshot =
                runtimeTuningState.snapshotCurrent();
        snapshotPersister.accept(snapshot);
        return snapshot;
    }

    private static void unsupportedPersistence(
            final RuntimeTuningSnapshot snapshot) {
        throw new UnsupportedOperationException(
                "Runtime tuning persistence is not available for this service.");
    }

}
