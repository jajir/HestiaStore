package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.directory.Directory;

public class SegmentPropertiesController {

    private final Directory directory;
    private final SegmentId id;
    private final OptimisticLockObjectVersionProvider versionProvider;
    private OptimisticLock lock;
    private SegmentPropertiesManager segmenPropertiesManager;

    SegmentPropertiesController(final Directory directory, final SegmentId id,
            final OptimisticLockObjectVersionProvider versionProvider) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        lock = new OptimisticLock(versionProvider);
    }

    public SegmentPropertiesManager getSegmentPropertiesManager() {
        if (lock.isLocked()) {
            segmenPropertiesManager = null;
        }
        if (segmenPropertiesManager == null) {
            segmenPropertiesManager = new SegmentPropertiesManager(directory,
                    id);
            lock = new OptimisticLock(versionProvider);
        }
        return segmenPropertiesManager;

    }

}
