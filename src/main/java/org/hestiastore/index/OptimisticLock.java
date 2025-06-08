package org.hestiastore.index;

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
        this.versionProvider = Vldtn.requireNonNull(versionProvider,
                "versionProvider");
        this.initialObjectVersion = versionProvider.getVersion();
    }

    public boolean isLocked() {
        return initialObjectVersion != versionProvider.getVersion();
    }

}
