package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class WalConfigurationTest {

    @Test
    void builderDefaultsToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder().build();

        assertSame(Wal.EMPTY, conf.wal());
    }

    @Test
    void walConfigurationNullIsNormalizedToWalEmpty() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.configuration(null)).build();

        assertSame(Wal.EMPTY, conf.wal());
    }
}
