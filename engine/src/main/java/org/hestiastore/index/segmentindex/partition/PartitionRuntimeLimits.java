package org.hestiastore.index.segmentindex.partition;

import org.hestiastore.index.Vldtn;

/**
 * Runtime limits for the partitioned ingest overlay.
 *
 * @author honza
 */
public final class PartitionRuntimeLimits {

    private final int maxNumberOfKeysInActivePartition;
    private final int maxNumberOfImmutableRunsPerPartition;
    private final int maxNumberOfKeysInPartitionBuffer;
    private final int maxNumberOfKeysInIndexBuffer;

    public PartitionRuntimeLimits(final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfImmutableRunsPerPartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int maxNumberOfKeysInIndexBuffer) {
        this.maxNumberOfKeysInActivePartition = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInActivePartition,
                "maxNumberOfKeysInActivePartition");
        this.maxNumberOfImmutableRunsPerPartition = Vldtn
                .requireGreaterThanZero(maxNumberOfImmutableRunsPerPartition,
                        "maxNumberOfImmutableRunsPerPartition");
        this.maxNumberOfKeysInPartitionBuffer = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInPartitionBuffer,
                "maxNumberOfKeysInPartitionBuffer");
        this.maxNumberOfKeysInIndexBuffer = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInIndexBuffer, "maxNumberOfKeysInIndexBuffer");
        if (maxNumberOfKeysInIndexBuffer < maxNumberOfKeysInPartitionBuffer) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInIndexBuffer must be >= maxNumberOfKeysInPartitionBuffer");
        }
    }

    public int getMaxNumberOfKeysInActivePartition() {
        return maxNumberOfKeysInActivePartition;
    }

    public int getMaxNumberOfImmutableRunsPerPartition() {
        return maxNumberOfImmutableRunsPerPartition;
    }

    public int getMaxNumberOfKeysInPartitionBuffer() {
        return maxNumberOfKeysInPartitionBuffer;
    }

    public int getMaxNumberOfKeysInIndexBuffer() {
        return maxNumberOfKeysInIndexBuffer;
    }
}
