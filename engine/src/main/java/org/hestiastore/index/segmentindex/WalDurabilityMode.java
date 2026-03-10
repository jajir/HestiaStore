package org.hestiastore.index.segmentindex;

/**
 * Durability policy used by WAL commits.
 */
public enum WalDurabilityMode {

    /**
     * Writes are acknowledged without waiting for group or explicit sync.
     */
    ASYNC,

    /**
     * Writes are acknowledged after a periodic group durable step.
     */
    GROUP_SYNC,

    /**
     * Writes are acknowledged only after synchronous durability step.
     */
    SYNC
}
