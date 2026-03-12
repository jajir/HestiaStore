package org.hestiastore.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class BenchmarkHistoryScriptsSmokeTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PROFILE = "segment-index-pr-smoke";
    private static final String BENCHMARK_GET_HIT = "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark.getHit";
    private static final String BENCHMARK_GET_MISS = "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark.getMiss";
    private static final String BENCHMARK_MIXED_PUT = "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark.putWorkload";

    @TempDir
    Path tempDir;

    @Test
    void compareScriptWritesOutputsAndCountsMetricStatuses()
            throws Exception {
        assumePython3Available();
        final Path baselineSummary = tempDir.resolve("baseline-summary.json");
        final Path candidateSummary = tempDir.resolve("candidate-summary.json");
        final Path markdownOut = tempDir.resolve("comparison.md");
        final Path jsonOut = tempDir.resolve("comparison.json");

        writeSummary(baselineSummary, baselineSummaryModel());
        writeSummary(candidateSummary, candidateSummaryModel());

        final ProcessResult result = runPythonScript(
                "compare_jmh_profile.py",
                "--baseline", baselineSummary.toString(),
                "--candidate", candidateSummary.toString(),
                "--markdown-out", markdownOut.toString(),
                "--json-out", jsonOut.toString());

        assertEquals(0, result.exitCode(), result.output());
        assertTrue(Files.isRegularFile(markdownOut));
        assertTrue(Files.isRegularFile(jsonOut));

        final JsonNode comparison = OBJECT_MAPPER.readTree(jsonOut.toFile());
        assertEquals(1, comparison.path("worseCount").asInt());
        assertEquals(1, comparison.path("newMetricCount").asInt());
        assertEquals(1, comparison.path("removedMetricCount").asInt());
        assertTrue(hasMetricWithStatus(comparison, "segment-index-get-overlay:getHit:overlayProbe",
                "better"));
        assertTrue(hasMetricWithStatus(comparison,
                "segment-index-mixed-drain:putWorkload", "new"));
        assertTrue(hasMetricWithStatus(comparison,
                "segment-index-get-persisted:getMiss", "removed"));

        final String markdown = Files.readString(markdownOut,
                StandardCharsets.UTF_8);
        assertTrue(markdown.contains("segment-index-get-overlay:getHit"));
        assertTrue(markdown.contains("segment-index-mixed-drain:putWorkload"));
        assertTrue(markdown.contains("segment-index-get-persisted:getMiss"));
    }

    @Test
    void compareScriptReturnsExitCodeTwoWhenFailOnRegressionIsEnabled()
            throws Exception {
        assumePython3Available();
        final Path baselineSummary = tempDir.resolve("baseline-fail.json");
        final Path candidateSummary = tempDir.resolve("candidate-fail.json");
        final Path markdownOut = tempDir.resolve("comparison-fail.md");
        final Path jsonOut = tempDir.resolve("comparison-fail.json");

        writeSummary(baselineSummary, baselineSummaryModel());
        writeSummary(candidateSummary, candidateSummaryModel());

        final ProcessResult result = runPythonScript(
                "compare_jmh_profile.py",
                "--baseline", baselineSummary.toString(),
                "--candidate", candidateSummary.toString(),
                "--markdown-out", markdownOut.toString(),
                "--json-out", jsonOut.toString(),
                "--fail-on-regression");

        assertEquals(2, result.exitCode(), result.output());
        assertTrue(Files.isRegularFile(markdownOut));
        assertTrue(Files.isRegularFile(jsonOut));
    }

    @Test
    void publishAndResolveScriptsRoundTripHistoryPointer() throws Exception {
        assumePython3Available();
        final Path sourceDir = tempDir.resolve("candidate-run");
        final Path historyDir = tempDir.resolve("perf-artifacts");
        final Path comparisonMarkdown = tempDir.resolve("comparison-vs-prev.md");
        final Path comparisonJson = tempDir.resolve("comparison-vs-prev.json");
        Files.createDirectories(sourceDir.resolve("raw"));
        Files.createDirectories(sourceDir.resolve("logs"));
        writeSummary(sourceDir.resolve("summary.json"), candidateSummaryModel());
        Files.writeString(sourceDir.resolve("raw/sample.json"), "{}",
                StandardCharsets.UTF_8);
        Files.writeString(sourceDir.resolve("logs/sample.log"), "ok\n",
                StandardCharsets.UTF_8);
        Files.writeString(comparisonMarkdown, "# Comparison\n",
                StandardCharsets.UTF_8);
        Files.writeString(comparisonJson, "{\"status\":\"ok\"}\n",
                StandardCharsets.UTF_8);

        final ProcessResult publishResult = runPythonScript(
                "publish_jmh_history.py",
                "--source-dir", sourceDir.toString(),
                "--history-dir", historyDir.toString(),
                "--channel", "main",
                "--run-suffix", "unit",
                "--comparison-markdown", comparisonMarkdown.toString(),
                "--comparison-json", comparisonJson.toString());

        assertEquals(0, publishResult.exitCode(), publishResult.output());

        final Path latestPointer = historyDir.resolve("history")
                .resolve(PROFILE).resolve("latest-main.json");
        assertTrue(Files.isRegularFile(latestPointer));
        final JsonNode pointer = OBJECT_MAPPER.readTree(latestPointer.toFile());
        final Path resolvedSummary = historyDir
                .resolve(pointer.path("summaryPath").asText());
        final Path resolvedMarkdown = historyDir
                .resolve(pointer.path("comparisonMarkdownPath").asText());
        final Path resolvedJson = historyDir
                .resolve(pointer.path("comparisonJsonPath").asText());
        assertTrue(Files.isRegularFile(resolvedSummary));
        assertTrue(Files.isRegularFile(resolvedMarkdown));
        assertTrue(Files.isRegularFile(resolvedJson));
        assertTrue(pointer.path("runPath").asText().contains("/2026/03/"));

        final ProcessResult resolveResult = runPythonScript(
                "resolve_jmh_history_baseline.py",
                "--history-dir", historyDir.toString(),
                "--profile", PROFILE,
                "--channel", "main");

        assertEquals(0, resolveResult.exitCode(), resolveResult.output());
        assertEquals(resolvedSummary.toRealPath().toString(),
                Path.of(resolveResult.output().trim()).toRealPath().toString());
    }

    private boolean hasMetricWithStatus(final JsonNode comparison,
            final String displayName, final String status) {
        for (final JsonNode metric : comparison.path("metrics")) {
            if (displayName.equals(metric.path("displayName").asText())
                    && status.equals(metric.path("status").asText())) {
                return true;
            }
        }
        return false;
    }

    private void assumePython3Available() throws Exception {
        final ProcessResult result = runCommand(List.of("python3", "--version"));
        Assumptions.assumeTrue(result.exitCode() == 0,
                "python3 is required for script smoke tests");
    }

    private ProcessResult runPythonScript(final String scriptName,
            final String... args) throws Exception {
        final List<String> command = new ArrayList<>();
        command.add("python3");
        command.add(scriptPath(scriptName).toString());
        command.addAll(List.of(args));
        return runCommand(command);
    }

    private ProcessResult runCommand(final List<String> command)
            throws IOException, InterruptedException {
        final Process process = new ProcessBuilder(command)
                .directory(repoRoot().toFile())
                .redirectErrorStream(true)
                .start();
        final String output = new String(process.getInputStream().readAllBytes(),
                StandardCharsets.UTF_8);
        final int exitCode = process.waitFor();
        return new ProcessResult(exitCode, output);
    }

    private void writeSummary(final Path path, final Map<String, Object> summary)
            throws IOException {
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(),
                summary);
    }

    private Map<String, Object> baselineSummaryModel() {
        return summary("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                List.of(
                        benchmark("segment-index-get-overlay",
                                BENCHMARK_GET_HIT, 100D,
                                Map.of("overlayProbe", metric(10D, "ops/s"))),
                        benchmark("segment-index-get-persisted",
                                BENCHMARK_GET_MISS, 90D, Map.of())));
    }

    private Map<String, Object> candidateSummaryModel() {
        return summary("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                List.of(
                        benchmark("segment-index-get-overlay",
                                BENCHMARK_GET_HIT, 80D,
                                Map.of("overlayProbe", metric(12D, "ops/s"))),
                        benchmark("segment-index-mixed-drain",
                                BENCHMARK_MIXED_PUT, 210D, Map.of())));
    }

    private Map<String, Object> summary(final String sha,
            final List<Map<String, Object>> benchmarks) {
        return Map.of(
                "profile", PROFILE,
                "description", "script smoke",
                "timestampUtc", "2026-03-12T10:11:12+00:00",
                "git", Map.of("sha", sha, "branch", "devel"),
                "benchmarks", benchmarks);
    }

    private Map<String, Object> benchmark(final String label,
            final String benchmarkName, final double primaryScore,
            final Map<String, Object> secondaryMetrics) {
        return Map.of(
                "label", label,
                "normalized", Map.of(
                        "results", List.of(Map.of(
                                "benchmark", benchmarkName,
                                "primaryMetric", metric(primaryScore, "ops/s"),
                                "secondaryMetrics",
                                new LinkedHashMap<>(secondaryMetrics)))));
    }

    private Map<String, Object> metric(final double score, final String unit) {
        return Map.of("score", Double.valueOf(score), "scoreUnit", unit);
    }

    private Path repoRoot() {
        final Path current = Path.of("").toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("benchmarks"))) {
            return current;
        }
        if (current.getFileName() != null
                && "benchmarks".equals(current.getFileName().toString())) {
            return current.getParent();
        }
        return current;
    }

    private Path scriptPath(final String scriptName) {
        return repoRoot().resolve("benchmarks").resolve("scripts")
                .resolve(scriptName);
    }

    private record ProcessResult(int exitCode, String output) {
    }
}
