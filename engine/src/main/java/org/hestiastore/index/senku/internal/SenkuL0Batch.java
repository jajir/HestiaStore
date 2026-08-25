package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * Coordinator-owned barrier and cached sparse metadata for one L0 batch.
 */
final class SenkuL0Batch {

    private final long[] inputFlushIds;
    private final LargeFile[] inputFlushFiles;
    private final long[][] packedStartPositionsByFlushAndShard;
    private final long[][] recordCountsByFlushAndShard;
    private final long[] outputRunIdsByShard;
    private final SenkuCompletedRun[] completedRunsByShard;

    private int nextShardToSubmit;
    private int completedShardCount;

    private SenkuL0Batch(final long[] inputFlushIds,
            final LargeFile[] inputFlushFiles,
            final long[][] packedStartPositionsByFlushAndShard,
            final long[][] recordCountsByFlushAndShard,
            final long[] outputRunIdsByShard) {
        this.inputFlushIds = inputFlushIds;
        this.inputFlushFiles = inputFlushFiles;
        this.packedStartPositionsByFlushAndShard =
                packedStartPositionsByFlushAndShard;
        this.recordCountsByFlushAndShard = recordCountsByFlushAndShard;
        this.outputRunIdsByShard = outputRunIdsByShard;
        completedRunsByShard = new SenkuCompletedRun[outputRunIdsByShard.length];
    }

    /**
     * Loads and validates the selected committed flush metadata exactly once.
     *
     * @param flushDirectory persistent flush parent
     * @param inputFlushIds selected flush generations in ascending order
     * @param outputRunIdsByShard reserved L0 run ID for every shard
     * @param shardCount configured shard count
     * @param maxEntriesPerPart configured physical part entry limit
     * @param dataBlockSize chunk-store block size
     * @return prepared batch
     */
    static SenkuL0Batch load(final Directory flushDirectory,
            final long[] inputFlushIds, final long[] outputRunIdsByShard,
            final int shardCount, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize) {
        final Directory validatedDirectory = Vldtn.requireNonNull(flushDirectory,
                "flushDirectory");
        final long[] flushIds = Arrays.copyOf(Vldtn
                .requireNonNull(inputFlushIds, "inputFlushIds"),
                inputFlushIds.length);
        Vldtn.requireGreaterThanZero(flushIds.length, "inputFlushCount");
        final int validatedShardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
        final long[] outputIds = Arrays.copyOf(Vldtn.requireNonNull(
                outputRunIdsByShard, "outputRunIdsByShard"),
                outputRunIdsByShard.length);
        Vldtn.requireTrue(outputIds.length == validatedShardCount,
                "One output run ID is required for every shard");
        validateIds(flushIds, outputIds);
        final LargeFile[] files = new LargeFile[flushIds.length];
        final long[][] positions = new long[flushIds.length][];
        final long[][] counts = new long[flushIds.length][];
        for (int input = 0; input < flushIds.length; input++) {
            final Directory source = openExistingFlush(validatedDirectory,
                    flushIds[input]);
            final int partCount = SenkuMetadataCodec
                    .readFlushPartCount(source);
            final LargeFile file = new LargeFile(source, dataBlockSize,
                    maxEntriesPerPart, partCount);
            try (LargeFileReader ignored = file.openReader()) {
                // Opening validates the exact physical part layout.
            }
            final SenkuShardIndex index = SenkuShardIndexCodec.read(source,
                    dataBlockSize, validatedShardCount);
            Vldtn.requireTrue(index.totalRecordCount() > 0L,
                    "A committed flush must contain at least one record");
            files[input] = file;
            positions[input] = new long[validatedShardCount];
            counts[input] = new long[validatedShardCount];
            for (int shardId = 0; shardId < validatedShardCount; shardId++) {
                positions[input][shardId] = index.packedPosition(shardId);
                counts[input][shardId] = index.recordCount(shardId);
            }
        }
        return new SenkuL0Batch(flushIds, files, positions, counts, outputIds);
    }

    /**
     * Returns and reserves the next pending shard in ascending order.
     *
     * @return next shard or empty after all shards were submitted
     */
    OptionalInt takeNextShard() {
        if (nextShardToSubmit == outputRunIdsByShard.length) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(nextShardToSubmit++);
    }

    boolean hasPendingShard() {
        return nextShardToSubmit < outputRunIdsByShard.length;
    }

    /**
     * Creates the at-most-fan-in non-empty ranges for one shard job.
     *
     * @param shardId configured shard ID
     * @return immutable source list
     */
    List<SenkuMergeSource> sourcesForShard(final int shardId) {
        Vldtn.requireBetween(shardId, 0, outputRunIdsByShard.length - 1,
                "shardId");
        final List<SenkuMergeSource> sources = new ArrayList<>(
                inputFlushFiles.length);
        for (int input = 0; input < inputFlushFiles.length; input++) {
            final long recordCount = recordCountsByFlushAndShard[input][shardId];
            if (recordCount > 0L) {
                sources.add(SenkuMergeSource.flush(inputFlushFiles[input],
                        LargeFilePosition.fromPacked(
                                packedStartPositionsByFlushAndShard[input][shardId]),
                        recordCount));
            }
        }
        return List.copyOf(sources);
    }

    /**
     * Accepts one submitted shard result; inputs remain owned by the batch.
     *
     * @param completedRun worker result
     */
    void accept(final SenkuCompletedRun completedRun) {
        final SenkuCompletedRun completed = Vldtn.requireNonNull(completedRun,
                "completedRun");
        final int shardId = completed.shardId();
        Vldtn.requireBetween(shardId, 0, outputRunIdsByShard.length - 1,
                "shardId");
        if (shardId >= nextShardToSubmit) {
            throw new IndexException(
                    "Cannot accept an L0 shard that was not submitted.");
        }
        if (completed.level() != 0
                || completed.runId() != outputRunIdsByShard[shardId]) {
            throw new IndexException("Completed run does not match the L0 batch output.");
        }
        if (completedRunsByShard[shardId] != null) {
            throw new IndexException("L0 shard result was already accepted.");
        }
        completedRunsByShard[shardId] = completed;
        completedShardCount++;
    }

    boolean isComplete() {
        return completedShardCount == completedRunsByShard.length;
    }

    int shardCount() {
        return completedRunsByShard.length;
    }

    SenkuCompletedRun completedRun(final int shardId) {
        Vldtn.requireBetween(shardId, 0, completedRunsByShard.length - 1,
                "shardId");
        final SenkuCompletedRun completed = completedRunsByShard[shardId];
        if (completed == null) {
            throw new IndexException("L0 shard result is not complete.");
        }
        return completed;
    }

    long outputRunId(final int shardId) {
        Vldtn.requireBetween(shardId, 0, outputRunIdsByShard.length - 1,
                "shardId");
        return outputRunIdsByShard[shardId];
    }

    long[] inputFlushIds() {
        return Arrays.copyOf(inputFlushIds, inputFlushIds.length);
    }

    private static Directory openExistingFlush(final Directory flushDirectory,
            final long flushId) {
        final String name = SenkuFileNames.flushDirectory(flushId);
        if (!flushDirectory.isFileExists(name)) {
            throw new IndexException("Missing selected flush directory '" + name
                    + "'.");
        }
        return flushDirectory.openSubDirectory(name);
    }

    private static void validateIds(final long[] flushIds,
            final long[] outputIds) {
        long previous = -1L;
        for (final long flushId : flushIds) {
            Vldtn.requireGreaterThanOrEqualToZero(flushId, "flushId");
            Vldtn.requireTrue(flushId > previous,
                    "Flush IDs must be unique and strictly increasing");
            previous = flushId;
        }
        for (final long outputId : outputIds) {
            Vldtn.requireGreaterThanOrEqualToZero(outputId, "outputRunId");
        }
    }
}
