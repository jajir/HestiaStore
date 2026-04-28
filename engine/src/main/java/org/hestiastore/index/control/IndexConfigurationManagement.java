package org.hestiastore.index.control;

import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeTuningPatch;
import org.hestiastore.index.control.model.RuntimeTuningSnapshot;

/**
 * Runtime configuration management API for one index.
 */
public interface IndexConfigurationManagement {

    /**
     * Returns current runtime configuration snapshot.
     *
     * @return configuration snapshot
     */
    ConfigurationSnapshot getConfigurationActual();

    /**
     * Returns original configuration values loaded at startup.
     *
     * @return original configuration values
     */
    ConfigurationSnapshot getConfigurationOriginal();

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
    default RuntimeTuningSnapshot getRuntimeTuningActual() {
        return RuntimeTuningSnapshot.from(getConfigurationActual());
    }

    /**
     * Returns original runtime tuning snapshot as a typed view.
     *
     * @return typed original runtime tuning snapshot
     */
    default RuntimeTuningSnapshot getRuntimeTuningOriginal() {
        return RuntimeTuningSnapshot.from(getConfigurationOriginal());
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
