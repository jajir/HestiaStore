package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.junit.jupiter.api.Test;

class SenkuMaintenanceMergeBenchmarkTest {
    @Test
    void completeRankedFixturesPreserveExactUniqueKeys() {
        for (final int sources : new int[] { 4, 64 }) {
            for (final int duplicates : new int[] { 0, 50 }) {
                final SenkuMaintenanceMergeBenchmark benchmark = new SenkuMaintenanceMergeBenchmark();
                benchmark.entryCount = 128;
                benchmark.sourceCount = sources;
                benchmark.duplicatePercent = duplicates;
                benchmark.setupTrial();
                benchmark.setupInvocation();
                final long count = duplicates == 0 ? 128 : 64;
                assertEquals(count, benchmark.merge());
                final SenkuRunManifest manifest = SenkuMetadataCodec
                        .readRunManifest(benchmark.output);
                final SenkuMergeSource source = SenkuMergeSource
                        .run(new LargeFile(benchmark.output,
                                SenkuMaintenanceMergeBenchmark.BLOCK_SIZE,
                                10_000_000L, manifest.partCount()), count);
                try (var reader = source.open(new TypeDescriptorLong(),
                        new TypeDescriptorNull(), benchmark.codec())) {
                    for (long ordinal = 0; ordinal < count; ordinal++) {
                        assertEquals(benchmark.codec()
                                .decodeLongKey(SenkuMaintenanceMergeBenchmark
                                        .encodedRank(ordinal)),
                                reader.next().getKey().longValue());
                    }
                    assertFalse(reader.hasNext());
                }
            }
        }
    }

    @Test
    void rejectsInvalidFixtureBeforePublishingInputs() {
        final SenkuMaintenanceMergeBenchmark benchmark = new SenkuMaintenanceMergeBenchmark();
        benchmark.entryCount = 65;
        assertThrows(IllegalArgumentException.class, benchmark::setupTrial);
        benchmark.entryCount = 128;
        benchmark.duplicatePercent = 17;
        assertThrows(IllegalArgumentException.class, benchmark::setupTrial);
    }
}
