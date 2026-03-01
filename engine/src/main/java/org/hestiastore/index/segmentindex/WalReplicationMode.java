package org.hestiastore.index.segmentindex;

/**
 * Replication mode for WAL records.
 */
public enum WalReplicationMode {

    /**
     * Local-only WAL. Replication and fencing are disabled.
     */
    DISABLED,

    /**
     * Leader mode for shipping records to followers.
     */
    LEADER,

    /**
     * Follower mode for accepting replicated records.
     */
    FOLLOWER
}
