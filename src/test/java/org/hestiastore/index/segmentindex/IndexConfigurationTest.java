package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.Test;

class IndexConfigurationTest {

    @Test
    void filterListsAreDefensiveAndImmutable() {
        final List<ChunkFilter> encoding = new ArrayList<>();
        encoding.add(new ChunkFilterDoNothing());
        final List<ChunkFilter> decoding = new ArrayList<>();
        decoding.add(new ChunkFilterDoNothing());

        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withEncodingFilters(encoding)
                .withDecodingFilters(decoding)
                .build();

        encoding.add(new ChunkFilterDoNothing());
        decoding.add(new ChunkFilterDoNothing());

        assertEquals(1, config.getEncodingChunkFilters().size());
        assertEquals(1, config.getDecodingChunkFilters().size());
        assertThrows(UnsupportedOperationException.class,
                () -> config.getEncodingChunkFilters()
                        .add(new ChunkFilterDoNothing()));
        assertThrows(UnsupportedOperationException.class,
                () -> config.getDecodingChunkFilters()
                        .add(new ChunkFilterDoNothing()));
    }

    @Test
    void maintenanceExecutorIsExposed() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final IndexConfiguration<Integer, String> config = IndexConfiguration
                    .<Integer, String>builder()
                    .withMaintenanceExecutor(executor)
                    .build();

            assertSame(executor, config.getMaintenanceExecutor());
        } finally {
            executor.shutdownNow();
        }
    }
}
