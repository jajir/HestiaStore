package org.hestiastore.index.segmentindex;

/**
 * Action performed when invalid WAL tail is detected during recovery.
 */
public enum WalCorruptionPolicy {

    /**
     * Truncate WAL to the last valid record and continue recovery.
     */
    TRUNCATE_INVALID_TAIL,

    /**
     * Fail startup when invalid WAL data is detected.
     */
    FAIL_FAST
}
