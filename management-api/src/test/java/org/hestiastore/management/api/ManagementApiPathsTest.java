package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ManagementApiPathsTest {

    @Test
    void allPathsUseV1Prefix() {
        assertTrue(ManagementApiPaths.STATE.startsWith(ManagementApiPaths.BASE));
        assertTrue(
                ManagementApiPaths.METRICS.startsWith(ManagementApiPaths.BASE));
        assertTrue(ManagementApiPaths.ACTION_FLUSH
                .startsWith(ManagementApiPaths.BASE));
        assertTrue(ManagementApiPaths.ACTION_COMPACT
                .startsWith(ManagementApiPaths.BASE));
        assertTrue(
                ManagementApiPaths.CONFIG.startsWith(ManagementApiPaths.BASE));
    }
}
