package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Runtime tuning API for one index.
 */
public interface RuntimeTuning {

    /**
     * Returns current runtime tuning snapshot.
     *
     * @return runtime tuning snapshot
     */
    RuntimeTuningSnapshot current();

    /**
     * Returns original runtime tuning values loaded at startup.
     *
     * @return original runtime tuning values
     */
    RuntimeTuningSnapshot original();

    /**
     * Validates runtime tuning patch without side effects.
     *
     * @param patch patch values
     * @return validation result
     */
    RuntimeTuningValidation validate(RuntimeTuningPatch patch);

    /**
     * Applies runtime tuning patch.
     *
     * @param patch patch values
     * @return apply result
     */
    RuntimeTuningResult apply(RuntimeTuningPatch patch);

    /**
     * Persists the current runtime tuning snapshot.
     *
     * @return persisted runtime tuning snapshot
     */
    RuntimeTuningSnapshot persistCurrent();
}
