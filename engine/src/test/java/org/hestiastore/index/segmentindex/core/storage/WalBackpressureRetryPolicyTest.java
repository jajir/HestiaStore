package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class WalBackpressureRetryPolicyTest {

    @Test
    void constructorAcceptsValidValues() {
        assertNotNull(new WalBackpressureRetryPolicy(1, 10));
    }
}
