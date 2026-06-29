package org.hestiastore.index.segmentindex.wal;

/**
 * Read-only source of WAL runtime monitoring snapshots.
 */
public interface WalMonitoringView {

    /**
     * Returns a monitoring view for a disabled WAL runtime.
     *
     * @return disabled WAL monitoring view
     */
    static WalMonitoringView empty() {
        return WalMonitoring::empty;
    }

    /**
     * Returns the current immutable WAL monitoring snapshot.
     *
     * @return WAL monitoring snapshot
     */
    WalMonitoring statsSnapshot();
}
