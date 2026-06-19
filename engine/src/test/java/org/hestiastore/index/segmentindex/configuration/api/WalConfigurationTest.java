package org.hestiastore.index.segmentindex.configuration.api;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class WalConfigurationTest {

    @Test
    void builderDefaultsToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder().build();

        assertSame(IndexWalConfiguration.EMPTY, conf.wal());
        assertFalse(conf.isWalConfigured());
    }

    @Test
    void walConfigurationNullIsNormalizedToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.configuration(null)).build();

        assertSame(IndexWalConfiguration.EMPTY, conf.wal());
    }
}
