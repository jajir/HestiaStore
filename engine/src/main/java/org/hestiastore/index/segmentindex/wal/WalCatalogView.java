package org.hestiastore.index.segmentindex.wal;

import java.util.List;

final class WalCatalogView {

    private final long checkpointLsn;
    private final List<WalSegmentDescriptor> discoveredSegments;

    WalCatalogView(final long checkpointLsn,
            final List<WalSegmentDescriptor> discoveredSegments) {
        this.checkpointLsn = checkpointLsn;
        this.discoveredSegments = List.copyOf(discoveredSegments);
    }

    long checkpointLsn() {
        return checkpointLsn;
    }

    List<WalSegmentDescriptor> discoveredSegments() {
        return discoveredSegments;
    }
}
