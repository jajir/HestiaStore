package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuMergeReservationTest {

    @Test
    void l0ReservationExposesSharedInputsAndExecutesJob() {
        final SenkuMergeReservation<Integer, Long> reservation =
                SenkuMergeReservation.l0(emptyJob(2, 0, 3L),
                        List.of("flush/flush-00000"),
                        "shard-00002/level-00000/run-00003");

        final SenkuCompletedRun completed = reservation.execute();

        assertTrue(reservation.isL0());
        assertEquals(List.of("flush/flush-00000"),
                reservation.inputPaths());
        assertEquals(List.of(), reservation.runInputs());
        assertEquals(2, completed.shardId());
    }

    @Test
    void runReservationDerivesExactCanonicalInputPaths() {
        final SenkuRunSource input = new SenkuRunSource(new MemDirectory(), 1,
                2, 3L, new SenkuRunManifest(0, 0L));
        final SenkuMergeReservation<Integer, Long> reservation =
                SenkuMergeReservation.runs(emptyJob(1, 3, 4L), List.of(input),
                        "shard-00001/level-00003/run-00004");

        assertFalse(reservation.isL0());
        assertEquals(List.of("shard-00001/level-00002/run-00003"),
                reservation.inputPaths());
        assertEquals(List.of(input), reservation.runInputs());
    }

    @Test
    void factoriesRejectNullElements() {
        final List<String> nullPath = Arrays.asList((String) null);
        final List<SenkuRunSource> nullRun = Arrays
                .asList((SenkuRunSource) null);

        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeReservation.l0(emptyJob(0, 0, 0L), nullPath,
                        "output"));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeReservation.runs(emptyJob(0, 1, 0L), nullRun,
                        "output"));
    }

    private static SenkuMergeJob<Integer, Long> emptyJob(final int shardId,
            final int level, final long runId) {
        return new SenkuMergeJob<>(List.of(), new MemDirectory(), shardId,
                level, runId, new TypeDescriptorInteger(),
                new TypeDescriptorLong(),
                (key, first, second) -> first + second, 1, 1L,
                DATA_BLOCK_SIZE);
    }
}
