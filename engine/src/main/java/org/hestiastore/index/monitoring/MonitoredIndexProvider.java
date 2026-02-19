package org.hestiastore.index.monitoring;

import java.util.List;

/**
 * Provider of currently monitored indexes.
 */
public interface MonitoredIndexProvider {

    /**
     * Returns an immutable snapshot of currently monitored indexes.
     *
     * @return monitored index list
     */
    List<? extends MonitoredIndex> monitoredIndexes();
}
