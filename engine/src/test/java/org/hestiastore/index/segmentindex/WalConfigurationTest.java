package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class WalConfigurationTest {

    @Test
    void builderDefaultsToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder().build();

        assertSame(Wal.EMPTY, conf.getWal());
    }

    @Test
    void withWalNullIsNormalizedToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder().withWal(null).build();

        assertSame(Wal.EMPTY, conf.getWal());
    }

    @Test
    void walBuilderDefaultsToLocalReplicationDisabled() {
        final Wal wal = Wal.builder().withEnabled(true).build();

        assertEquals(WalReplicationMode.DISABLED, wal.getReplicationMode());
        assertEquals("", wal.getSourceNodeId());
    }
}
