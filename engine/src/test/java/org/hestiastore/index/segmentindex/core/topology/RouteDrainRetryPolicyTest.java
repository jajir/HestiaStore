package org.hestiastore.index.segmentindex.core.topology;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class RouteDrainRetryPolicyTest {

    @Test
    void constructorAcceptsValidValues() {
        assertNotNull(new RouteDrainRetryPolicy(1, 10));
    }
}
