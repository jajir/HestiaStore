package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SplitRetryPolicyTest {

    @Test
    void constructorAcceptsValidValues() {
        assertNotNull(new SplitRetryPolicy(1, 10));
    }
}
