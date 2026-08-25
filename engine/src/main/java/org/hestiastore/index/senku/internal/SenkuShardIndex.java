package org.hestiastore.index.senku.internal;

import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Immutable primitive sparse positions and record counts for one flush.
 */
final class SenkuShardIndex {

    private final long[] packedPositions;
    private final long[] recordCounts;

    SenkuShardIndex(final long[] packedPositions, final long[] recordCounts) {
        final long[] validatedPositions = Vldtn.requireNonNull(packedPositions,
                "packedPositions");
        final long[] validatedCounts = Vldtn.requireNonNull(recordCounts,
                "recordCounts");
        Vldtn.requireGreaterThanZero(validatedPositions.length, "shardCount");
        Vldtn.requireTrue(validatedPositions.length == validatedCounts.length,
                "Shard positions and record counts must have the same length");
        this.packedPositions = Arrays.copyOf(validatedPositions,
                validatedPositions.length);
        this.recordCounts = Arrays.copyOf(validatedCounts,
                validatedCounts.length);
        validateEntries();
    }

    int shardCount() {
        return recordCounts.length;
    }

    LargeFilePosition position(final int shardId) {
        return LargeFilePosition.fromPacked(packedPosition(shardId));
    }

    long packedPosition(final int shardId) {
        validateShardId(shardId);
        return packedPositions[shardId];
    }

    long recordCount(final int shardId) {
        validateShardId(shardId);
        return recordCounts[shardId];
    }

    long totalRecordCount() {
        long total = 0L;
        try {
            for (final long recordCount : recordCounts) {
                total = Math.addExact(total, recordCount);
            }
        } catch (ArithmeticException e) {
            throw new IndexException("Shard record-count sum overflow.", e);
        }
        return total;
    }

    private void validateEntries() {
        for (int shardId = 0; shardId < recordCounts.length; shardId++) {
            Vldtn.requireGreaterThanOrEqualToZero(packedPositions[shardId],
                    "packedPosition");
            Vldtn.requireGreaterThanOrEqualToZero(recordCounts[shardId],
                    "recordCount");
        }
    }

    private void validateShardId(final int shardId) {
        Vldtn.requireBetween(shardId, 0, shardCount() - 1, "shardId");
    }
}
