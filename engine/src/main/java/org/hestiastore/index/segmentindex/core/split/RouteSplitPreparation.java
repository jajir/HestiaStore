package org.hestiastore.index.segmentindex.core.split;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;

/**
 * Result of preparing a route split from a parent segment snapshot.
 *
 * @param <K> key type
 */
final class RouteSplitPreparation<K> {

    private final RouteSplitPreparationStatus status;
    private final RouteSplitPlan<K> routeSplit;

    private RouteSplitPreparation(final RouteSplitPreparationStatus status,
            final RouteSplitPlan<K> routeSplit) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.routeSplit = routeSplit;
    }

    /**
     * Creates a prepared split result.
     *
     * @param routeSplit materialized route split
     * @param <K>        key type
     * @return prepared split result
     */
    static <K> RouteSplitPreparation<K> prepared(
            final RouteSplitPlan<K> routeSplit) {
        return new RouteSplitPreparation<>(RouteSplitPreparationStatus.PREPARED,
                Vldtn.requireNonNull(routeSplit, "routeSplit"));
    }

    /**
     * Creates a skipped split result.
     *
     * @param <K> key type
     * @return skipped split result
     */
    static <K> RouteSplitPreparation<K> skipped() {
        return new RouteSplitPreparation<>(RouteSplitPreparationStatus.SKIPPED,
                null);
    }

    /**
     * Creates a result requesting parent compaction.
     *
     * @param <K> key type
     * @return compact-parent result
     */
    static <K> RouteSplitPreparation<K> compactParent() {
        return new RouteSplitPreparation<>(
                RouteSplitPreparationStatus.COMPACT_PARENT, null);
    }

    /**
     * Returns the preparation outcome status.
     *
     * @return preparation status
     */
    RouteSplitPreparationStatus status() {
        return status;
    }

    /**
     * Returns the materialized split when status is {@code PREPARED}.
     *
     * @return optional route split
     */
    Optional<RouteSplitPlan<K>> routeSplit() {
        return Optional.ofNullable(routeSplit);
    }
}
