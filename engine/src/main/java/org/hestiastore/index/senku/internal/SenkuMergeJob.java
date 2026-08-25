package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * One immutable executable merge from exact sources to one planned run.
 */
final class SenkuMergeJob<K, V> {

    private final List<SenkuMergeSource> sources;
    private final Directory outputDirectory;
    private final int shardId;
    private final int outputLevel;
    private final long outputRunId;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final Comparator<? super K> keyComparator;
    private final SenkuMergeFunction<K, V> mergeFunction;
    private final int maxKeysPerPage;
    private final long maxEntriesPerPart;
    private final DataBlockSize dataBlockSize;
    private final BooleanSupplier publicationAllowed;

    /**
     * Creates a job for one already-reserved output directory.
     *
     * @param sources exact immutable input ranges
     * @param outputDirectory unique empty output run directory
     * @param shardId output shard ID
     * @param outputLevel output numeric level
     * @param outputRunId output run ID within the shard and level
     * @param keyTypeDescriptor key codec and comparator
     * @param valueTypeDescriptor value codec
     * @param mergeFunction duplicate-key reducer
     * @param maxKeysPerPage maximum output entries per page
     * @param maxEntriesPerPart maximum output entries per physical part
     * @param dataBlockSize chunk-store block size
     */
    SenkuMergeJob(final List<SenkuMergeSource> sources,
            final Directory outputDirectory, final int shardId,
            final int outputLevel, final long outputRunId,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize) {
        this(sources, outputDirectory, shardId, outputLevel, outputRunId,
                keyTypeDescriptor, valueTypeDescriptor, mergeFunction,
                maxKeysPerPage, maxEntriesPerPart, dataBlockSize, () -> true);
    }

    SenkuMergeJob(final List<SenkuMergeSource> sources,
            final Directory outputDirectory, final int shardId,
            final int outputLevel, final long outputRunId,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final BooleanSupplier publicationAllowed) {
        final List<SenkuMergeSource> validatedSources = Vldtn
                .requireNonNull(sources, "sources");
        validatedSources
                .forEach(source -> Vldtn.requireNonNull(source, "source"));
        this.sources = List.copyOf(validatedSources);
        this.outputDirectory = Vldtn.requireNonNull(outputDirectory,
                "outputDirectory");
        this.shardId = Vldtn.requireGreaterThanOrEqualToZero(shardId,
                "shardId");
        this.outputLevel = Vldtn.requireGreaterThanOrEqualToZero(outputLevel,
                "outputLevel");
        this.outputRunId = Vldtn.requireGreaterThanOrEqualToZero(outputRunId,
                "outputRunId");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        keyComparator = keyTypeDescriptor.getComparator();
        this.mergeFunction = Vldtn.requireNonNull(mergeFunction,
                "mergeFunction");
        this.maxKeysPerPage = Vldtn.requireGreaterThanZero(maxKeysPerPage,
                "maxKeysPerPage");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(
                maxEntriesPerPart, "maxEntriesPerPart");
        Vldtn.requireTrue(maxEntriesPerPart >= maxKeysPerPage,
                "maxEntriesPerPart must be greater than or equal to maxKeysPerPage");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.publicationAllowed = Vldtn.requireNonNull(publicationAllowed,
                "publicationAllowed");
    }

    /**
     * Creates one same-shard, same-level merge or one-input promotion job.
     *
     * @param sources committed run sources; at least one is required
     * @param outputDirectory unique empty output run directory
     * @param outputRunId output run ID in the next level
     * @param keyTypeDescriptor key codec and comparator
     * @param valueTypeDescriptor value codec
     * @param mergeFunction duplicate-key reducer
     * @param maxKeysPerPage maximum output entries per page
     * @param maxEntriesPerPart maximum output entries per physical part
     * @param dataBlockSize chunk-store block size
     * @return validated merge job
     */
    static <K, V> SenkuMergeJob<K, V> forRuns(
            final List<SenkuRunSource> sources,
            final Directory outputDirectory, final long outputRunId,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize) {
        return forRuns(sources, outputDirectory, outputRunId,
                keyTypeDescriptor, valueTypeDescriptor, mergeFunction,
                maxKeysPerPage, maxEntriesPerPart, dataBlockSize, () -> true);
    }

    static <K, V> SenkuMergeJob<K, V> forRuns(
            final List<SenkuRunSource> sources,
            final Directory outputDirectory, final long outputRunId,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final BooleanSupplier publicationAllowed) {
        final List<SenkuRunSource> validatedSources = Vldtn
                .requireNonNull(sources, "sources");
        Vldtn.requireGreaterThanZero(validatedSources.size(), "sourceCount");
        final SenkuRunSource first = Vldtn.requireNonNull(
                validatedSources.get(0), "source");
        final List<SenkuMergeSource> mergeSources = new ArrayList<>(
                validatedSources.size());
        for (final SenkuRunSource source : validatedSources) {
            final SenkuRunSource validated = Vldtn.requireNonNull(source,
                    "source");
            Vldtn.requireTrue(validated.shardId() == first.shardId(),
                    "Sorted-run merge sources must belong to one shard");
            Vldtn.requireTrue(validated.level() == first.level(),
                    "Sorted-run merge sources must belong to one level");
            mergeSources.add(validated.mergeSource(dataBlockSize,
                    maxEntriesPerPart));
        }
        final int outputLevel;
        try {
            outputLevel = Math.incrementExact(first.level());
        } catch (ArithmeticException e) {
            throw new IndexException("Sorted-run level overflow.", e);
        }
        return new SenkuMergeJob<>(mergeSources, outputDirectory,
                first.shardId(), outputLevel, outputRunId, keyTypeDescriptor,
                valueTypeDescriptor, mergeFunction, maxKeysPerPage,
                maxEntriesPerPart, dataBlockSize, publicationAllowed);
    }

    /**
     * Executes the streaming merge and publishes the output manifest.
     *
     * @return immutable completed-run result
     */
    SenkuCompletedRun execute() {
        final List<EntryIterator<K, V>> inputs = openInputs();
        final SenkuMergedEntryIterator<K, V> merged;
        try {
            merged = new SenkuMergedEntryIterator<>(inputs, keyComparator,
                    mergeFunction);
        } catch (Exception e) {
            closeInputs(inputs, e);
            throw asIndexException(e);
        }
        final SenkuRunManifest manifest = new SenkuRunWriter<>(outputDirectory,
                keyTypeDescriptor, valueTypeDescriptor, maxKeysPerPage,
                maxEntriesPerPart, dataBlockSize, publicationAllowed)
                .write(merged);
        return new SenkuCompletedRun(shardId, outputLevel, outputRunId,
                manifest);
    }

    private List<EntryIterator<K, V>> openInputs() {
        final List<EntryIterator<K, V>> inputs = new ArrayList<>(sources.size());
        try {
            for (final SenkuMergeSource source : sources) {
                inputs.add(source.open(keyTypeDescriptor, valueTypeDescriptor));
            }
            return inputs;
        } catch (Exception e) {
            closeInputs(inputs, e);
            throw asIndexException(e);
        }
    }

    private static void closeInputs(final List<? extends EntryIterator<?, ?>> inputs,
            final Exception primary) {
        for (final EntryIterator<?, ?> input : inputs) {
            if (input.wasClosed()) {
                continue;
            }
            try {
                input.close();
            } catch (Exception cleanupFailure) {
                primary.addSuppressed(cleanupFailure);
            }
        }
    }

    private static IndexException asIndexException(final Exception cause) {
        return cause instanceof IndexException ? (IndexException) cause
                : new IndexException("Unable to execute Senku merge job.",
                        cause);
    }
}
