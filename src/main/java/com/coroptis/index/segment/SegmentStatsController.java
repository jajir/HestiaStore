package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.directory.Directory;

public class SegmentStatsController {

    private final Directory directory;
    private final SegmentId id;
    private final OptimisticLockObjectVersionProvider versionProvider;
    private OptimisticLock lock;
    private SegmentStatsManager segmentStatsManager;

    SegmentStatsController(final Directory directory, final SegmentId id,
            final OptimisticLockObjectVersionProvider versionProvider) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        lock = new OptimisticLock(versionProvider);
    }

    public SegmentStatsManager getSegmentStatsManager() {
        if (lock.isLocked()) {
            segmentStatsManager = null;
        }
        if (segmentStatsManager == null) {
            segmentStatsManager = new SegmentStatsManager(directory, id);
            lock = new OptimisticLock(versionProvider);
        }
        return segmentStatsManager;

    }

}
