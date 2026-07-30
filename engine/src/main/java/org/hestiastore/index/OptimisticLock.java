package org.hestiastore.index;

import java.util.function.IntSupplier;

/**
 * Allows to use some locked object until it change.
 * 
 * @author honza
 *
 */
public class OptimisticLock {

    private final IntSupplier versionProvider;
    private final int initialObjectVersion;

    /**
     * Creates an optimistic lock snapshot using the supplied version source.
     *
     * @param versionProvider current object version supplier
     */
    public OptimisticLock(final IntSupplier versionProvider) {
        this.versionProvider = Vldtn.requireNonNull(versionProvider,
                "versionProvider");
        this.initialObjectVersion = versionProvider.getAsInt();
    }

    /**
     * Returns whether the supplied version has changed since construction.
     *
     * @return true when the observed version changed
     */
    public boolean isLocked() {
        return initialObjectVersion != versionProvider.getAsInt();
    }

}
