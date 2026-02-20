package org.hestiastore.index.control;

import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;

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
}
