package org.hestiastore.index.segmentindex.monitoring.model;

import org.hestiastore.index.Vldtn;

final class MetricModelValidation {

    private MetricModelValidation() {
    }

    static int nonNegative(final int value, final String name) {
        return Vldtn.requireGreaterThanOrEqualToZero(value, name);
    }

    static long nonNegative(final long value, final String name) {
        return Vldtn.requireGreaterThanOrEqualToZero(value, name);
    }
}
