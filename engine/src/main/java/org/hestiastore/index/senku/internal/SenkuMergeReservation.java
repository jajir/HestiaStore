package org.hestiastore.index.senku.internal;

import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Exact input and output reservation for one submitted merge job.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SenkuMergeReservation<K, V> {

    private final SenkuMergeJob<K, V> job;
    private final List<String> inputPaths;
    private final String outputPath;
    private final List<SenkuRunSource> runInputs;
    private final boolean l0;

    private SenkuMergeReservation(final SenkuMergeJob<K, V> job,
            final List<String> inputPaths, final String outputPath,
            final List<SenkuRunSource> runInputs, final boolean l0) {
        this.job = Vldtn.requireNonNull(job, "job");
        final List<String> validatedPaths = Vldtn.requireNonNull(inputPaths,
                "inputPaths");
        validatedPaths
                .forEach(path -> Vldtn.requireNonNull(path, "inputPath"));
        this.inputPaths = List.copyOf(validatedPaths);
        this.outputPath = Vldtn.requireNonNull(outputPath, "outputPath");
        final List<SenkuRunSource> validatedInputs = Vldtn
                .requireNonNull(runInputs, "runInputs");
        validatedInputs
                .forEach(input -> Vldtn.requireNonNull(input, "runInput"));
        this.runInputs = List.copyOf(validatedInputs);
        this.l0 = l0;
    }

    /**
     * Creates an L0 shard-job reservation sharing its batch inputs.
     *
     * @param job executable job
     * @param inputPaths shared flush input paths
     * @param outputPath unique planned run path
     * @return reservation
     */
    static <K, V> SenkuMergeReservation<K, V> l0(
            final SenkuMergeJob<K, V> job, final List<String> inputPaths,
            final String outputPath) {
        return new SenkuMergeReservation<>(job, inputPaths, outputPath,
                List.of(), true);
    }

    /**
     * Creates a same-level sorted-run reservation.
     *
     * @param job executable job
     * @param runInputs exact input runs
     * @param outputPath unique planned run path
     * @return reservation
     */
    static <K, V> SenkuMergeReservation<K, V> runs(
            final SenkuMergeJob<K, V> job,
            final List<SenkuRunSource> runInputs, final String outputPath) {
        final List<SenkuRunSource> inputs = Vldtn.requireNonNull(runInputs,
                "runInputs");
        inputs.forEach(input -> Vldtn.requireNonNull(input, "runInput"));
        final List<String> paths = inputs.stream()
                .map(SenkuSourceCatalog::runPath).toList();
        return new SenkuMergeReservation<>(job, paths, outputPath, inputs,
                false);
    }

    SenkuCompletedRun execute() {
        return job.execute();
    }

    List<String> inputPaths() {
        return inputPaths;
    }

    String outputPath() {
        return outputPath;
    }

    List<SenkuRunSource> runInputs() {
        return runInputs;
    }

    boolean isL0() {
        return l0;
    }
}
