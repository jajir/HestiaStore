package org.hestiastore.index.segmentindex.configuration.user;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class WalConfigurationTest {

    @Test
    void builderDefaultsToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder().build();

        assertNull(conf.wal());
    }

    @Test
    void walConfigurationNullIsNormalizedToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.configuration(null)).build();

        assertSame(IndexWalConfiguration.EMPTY, conf.wal());
    }
}
