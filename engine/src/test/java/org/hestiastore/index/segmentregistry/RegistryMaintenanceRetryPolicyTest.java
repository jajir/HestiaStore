package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class RegistryMaintenanceRetryPolicyTest {

    @Test
    void constructorAcceptsValidValues() {
        assertNotNull(new RegistryMaintenanceRetryPolicy(1, 10));
    }
}
