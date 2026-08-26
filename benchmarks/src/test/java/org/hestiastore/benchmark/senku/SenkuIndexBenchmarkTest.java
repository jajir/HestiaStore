package org.hestiastore.benchmark.senku;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class SenkuIndexBenchmarkTest {

    private final SenkuIndexBenchmark benchmark = new SenkuIndexBenchmark();

    @Test
    void ingestionBenchmarkCompletesItsLifecycle() {
        final SenkuIndexBenchmark.IngestState state =
                new SenkuIndexBenchmark.IngestState();

        assertDoesNotThrow(() -> {
            state.setup();
            for (int key = 0; key < 20; key++) {
                state.put(key);
            }
            state.tearDown();
        });
    }

    @Test
    void ingestionThreadStateDefaultsToUniqueSequentialKeys() {
        final SenkuIndexBenchmark.IngestThreadState state =
                new SenkuIndexBenchmark.IngestThreadState();

        assertEquals(0, state.nextKey());
        assertEquals(1, state.nextKey());
    }

    @Test
    void synchronousFlushBenchmarkCompletesItsLifecycle() {
        final SenkuIndexBenchmark.SynchronousFlushState state =
                new SenkuIndexBenchmark.SynchronousFlushState();

        assertDoesNotThrow(() -> {
            state.setup();
            benchmark.synchronousFlush(state);
            state.tearDown();
        });
    }

    @Test
    void maintenanceBenchmarkCoversL0AndRecursiveRunMerge() {
        assertDoesNotThrow(() -> {
            runMaintenance("flush-to-l0");
            runMaintenance("run-merge");
        });
    }

    @Test
    void readyStreamBenchmarkReturnsEveryEntry() {
        final SenkuIndexBenchmark.ReadyState state =
                new SenkuIndexBenchmark.ReadyState();
        state.entryCount = 40;
        state.setup();

        final long count = benchmark.readyStream(state);

        state.tearDown();
        assertEquals(40L, count);
    }

    @Test
    void endToEndBenchmarkReturnsFirstSortedEntryForEveryShape() {
        final SenkuIndexBenchmark.EndToEndState state =
                new SenkuIndexBenchmark.EndToEndState();
        state.entryCount = 40;
        for (final String distribution : new String[] { "balanced", "skewed" }) {
            state.shardDistribution = distribution;
            for (final int duplicatePercent : new int[] { 0, 50 }) {
                state.duplicatePercent = duplicatePercent;
                final Entry<Integer, Long> first = benchmark
                        .ingestToFirstSortedResult(state);
                assertNotNull(first);
                assertEquals(0, first.getKey());
            }
        }
    }

    @Test
    void persistedBaselineContainsAllScenariosAndThreeMeasurements()
            throws IOException {
        final JsonNode results = new ObjectMapper().readTree(
                baselineResult().toFile());
        final Set<String> endToEndShapes = new HashSet<>();

        for (final JsonNode result : results) {
            assertEquals(3, result.path("primaryMetric").path("rawData")
                    .path(0).size());
            if (result.path("benchmark").asText()
                    .endsWith("ingestToFirstSortedResult")) {
                final JsonNode params = result.path("params");
                endToEndShapes.add(params.path("duplicatePercent").asText()
                        + '-' + params.path("shardDistribution").asText());
            }
        }

        assertEquals(9, results.size());
        assertEquals(Set.of("0-balanced", "0-skewed", "50-balanced",
                "50-skewed"), endToEndShapes);
    }

    private void runMaintenance(final String scenario) {
        final SenkuIndexBenchmark.MaintenanceState state =
                new SenkuIndexBenchmark.MaintenanceState();
        state.scenario = scenario;
        state.setup();
        assertNotNull(benchmark.finishMaintenance(state));
        state.tearDown();
    }

    private static Path baselineResult() {
        final Path current = Path.of("").toAbsolutePath().normalize();
        final Path moduleResult = current
                .resolve("results/senku-index-baseline.json");
        if (Files.isRegularFile(moduleResult)) {
            return moduleResult;
        }
        return current.resolve("benchmarks/results/senku-index-baseline.json");
    }
}
