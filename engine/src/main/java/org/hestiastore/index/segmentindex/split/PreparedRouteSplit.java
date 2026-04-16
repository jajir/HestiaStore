package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Vldtn;

/**
 * Holds a fully materialized route split that is ready to be published.
 *
 * @param <K> key type
 */
public final class PreparedRouteSplit<K> {

    private final RouteSplitPlan<K> plan;

    PreparedRouteSplit(final RouteSplitPlan<K> plan) {
        this.plan = Vldtn.requireNonNull(plan, "plan");
    }

    RouteSplitPlan<K> plan() {
        return plan;
    }
}
