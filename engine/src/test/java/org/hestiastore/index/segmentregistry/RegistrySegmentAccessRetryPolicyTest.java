package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class RegistrySegmentAccessRetryPolicyTest {

    @Test
    void constructorAcceptsValidValues() {
        assertNotNull(new RegistrySegmentAccessRetryPolicy(1, 10));
    }
}
