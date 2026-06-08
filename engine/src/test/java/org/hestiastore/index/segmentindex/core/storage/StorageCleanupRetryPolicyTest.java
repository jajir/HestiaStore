package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class StorageCleanupRetryPolicyTest {

    @Test
    void constructorAcceptsValidValues() {
        assertNotNull(new StorageCleanupRetryPolicy(1, 10));
    }
}
