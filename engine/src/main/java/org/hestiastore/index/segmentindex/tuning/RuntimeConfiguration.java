package org.hestiastore.index.segmentindex.tuning;

/**
 * Runtime configuration API for one index.
 */
public interface RuntimeConfiguration {

    /**
     * Returns current runtime configuration snapshot.
     *
     * @return configuration snapshot
     */
    ConfigurationSnapshot getCurrent();

    /**
     * Returns original configuration values loaded at startup.
     *
     * @return original configuration values
     */
    ConfigurationSnapshot getOriginal();

    /**
     * Validates runtime configuration patch without side effects.
     *
     * @param patch patch values
     * @return validation result
     */
    RuntimePatchValidation validate(RuntimeConfigPatch patch);

    /**
     * Applies runtime configuration patch.
     *
     * @param patch patch values
     * @return apply result
     */
    RuntimePatchResult apply(RuntimeConfigPatch patch);

    /**
     * Returns current runtime tuning snapshot as a typed view.
     *
     * @return typed current runtime tuning snapshot
     */
    default RuntimeTuningSnapshot getCurrentRuntimeTuning() {
        return RuntimeTuningSnapshot.from(getCurrent());
    }

    /**
     * Returns original runtime tuning snapshot as a typed view.
     *
     * @return typed original runtime tuning snapshot
     */
    default RuntimeTuningSnapshot getOriginalRuntimeTuning() {
        return RuntimeTuningSnapshot.from(getOriginal());
    }

    /**
     * Validates typed runtime tuning patch without side effects.
     *
     * @param patch typed runtime tuning patch
     * @return validation result
     */
    default RuntimePatchValidation validateRuntimeTuning(
            final RuntimeTuningPatch patch) {
        return validate(patch == null ? null : patch.toRuntimeConfigPatch());
    }

    /**
     * Applies typed runtime tuning patch.
     *
     * @param patch typed runtime tuning patch
     * @return apply result
     */
    default RuntimePatchResult applyRuntimeTuning(
            final RuntimeTuningPatch patch) {
        return apply(patch == null ? null : patch.toRuntimeConfigPatch());
    }
}
