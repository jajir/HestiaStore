package com.coroptis.index;

import java.util.Objects;

/**
 * Allows to use some locked object until it change.
 * 
 * @author honza
 *
 */
public class OptimisticLock {

    private final OptimisticLockObjectVersionProvider versionProvider;
    private final int initialObjectVersion;

    public OptimisticLock(
            final OptimisticLockObjectVersionProvider versionProvider) {
        this.versionProvider = Objects.requireNonNull(versionProvider);
        this.initialObjectVersion = versionProvider.getVersion();
    }

    boolean isLocked() {
        return initialObjectVersion != versionProvider.getVersion();
    }

}
