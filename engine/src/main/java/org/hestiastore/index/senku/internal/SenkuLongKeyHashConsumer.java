package org.hestiastore.index.senku.internal;

/** Consumes an exact primitive key and its cached configured routing hash. */
@FunctionalInterface
interface SenkuLongKeyHashConsumer {
    /**
     * Visits one immutable detached mapping without boxing or rerouting.
     *
     * @param key            exact primitive key
     * @param configuredHash original configured persistent-shard hash
     */
    void accept(long key, int configuredHash);
}
