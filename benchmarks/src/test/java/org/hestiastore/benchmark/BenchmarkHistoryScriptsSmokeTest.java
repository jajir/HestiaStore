package org.hestiastore.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

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
        assertFalse(hasMetricWithDisplayName(comparison,
                "segment-index-get-overlay:getHit:diag_fileBytesDelta"));
        assertTrue(hasMetricWithStatus(comparison,
                "segment-index-mixed-drain:putWorkload", "new"));
        assertTrue(hasMetricWithStatus(comparison,
                "segment-index-get-persisted:getMiss", "removed"));
        assertTrue(firstMetricByDisplayName(comparison,
                "segment-index-get-overlay:getHit")
                        .path("baselineScoreErrorPct").isNumber());
        assertTrue(firstMetricByDisplayName(comparison,
                "segment-index-get-persisted:getMiss")
                        .path("baselineScoreErrorPct").isNull());

        final String markdown = Files.readString(markdownOut,
                StandardCharsets.UTF_8);
        assertTrue(markdown.contains("segment-index-get-overlay:getHit"));
        assertFalse(markdown.contains("diag_fileBytesDelta"));
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
        final Path metadataJson = tempDir.resolve("publish-metadata.json");
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
                "--comparison-json", comparisonJson.toString(),
                "--metadata-out", metadataJson.toString());

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
        final JsonNode metadata = OBJECT_MAPPER.readTree(metadataJson.toFile());
        assertEquals(pointer.path("runPath").asText(),
                metadata.path("runPath").asText());
        assertEquals(pointer.path("summaryPath").asText(),
                metadata.path("summaryPath").asText());
        assertTrue(metadata.path("prNumber").isNull());

        final Path historyIndex = historyDir.resolve("history").resolve("index.json");
        final JsonNode index = OBJECT_MAPPER.readTree(historyIndex.toFile());
        assertEquals("main", index.path("profiles").path(PROFILE)
                .path("latestChannel").asText());
        assertEquals("history/" + PROFILE + "/latest-main.json",
                index.path("profiles").path(PROFILE)
                        .path("latestPointerPath").asText());
        assertEquals("history/" + PROFILE + "/latest-main.json",
                index.path("profiles").path(PROFILE).path("channels")
                        .path("main").path("latestPointerPath").asText());

        final ProcessResult resolveResult = runPythonScript(
                "resolve_jmh_history_baseline.py",
                "--history-dir", historyDir.toString(),
                "--profile", PROFILE,
                "--channel", "main");

        assertEquals(0, resolveResult.exitCode(), resolveResult.output());
        assertEquals(resolvedSummary.toRealPath().toString(),
                Path.of(resolveResult.output().trim()).toRealPath().toString());
    }

    @Test
    void resolveScriptRejectsIncompleteBaselineWhenProfileSpecRequiresMoreCoverage()
            throws Exception {
        assumePython3Available();
        final Path sourceDir = tempDir.resolve("candidate-run-incomplete");
        final Path historyDir = tempDir.resolve("perf-artifacts-incomplete");
        final Path profileSpec = tempDir.resolve("profile.json");
        Files.createDirectories(sourceDir.resolve("raw"));
        Files.createDirectories(sourceDir.resolve("logs"));
        writeSummary(sourceDir.resolve("summary.json"), baselineSummaryModel());
        Files.writeString(sourceDir.resolve("raw/sample.json"), "{}",
                StandardCharsets.UTF_8);
        Files.writeString(sourceDir.resolve("logs/sample.log"), "ok\n",
                StandardCharsets.UTF_8);
        Files.writeString(profileSpec, """
                {
                  "profile": "segment-index-pr-smoke",
                  "benchmarks": [
                    { "label": "segment-index-get-overlay" },
                    { "label": "segment-index-get-persisted" },
                    { "label": "segment-index-hot-partition-put" }
                  ]
                }
                """, StandardCharsets.UTF_8);

        final ProcessResult publishResult = runPythonScript(
                "publish_jmh_history.py",
                "--source-dir", sourceDir.toString(),
                "--history-dir", historyDir.toString(),
                "--channel", "main",
                "--run-suffix", "unit");

        assertEquals(0, publishResult.exitCode(), publishResult.output());

        final ProcessResult resolveResult = runPythonScript(
                "resolve_jmh_history_baseline.py",
                "--history-dir", historyDir.toString(),
                "--profile", PROFILE,
                "--profile-spec", profileSpec.toString(),
                "--channel", "main");

        assertEquals(1, resolveResult.exitCode(), resolveResult.output());
    }

    @Test
    void resolveScriptRejectsInvalidProfileSpec() throws Exception {
        assumePython3Available();
        final Path sourceDir = tempDir.resolve("candidate-run-invalid-spec");
        final Path historyDir = tempDir.resolve("perf-artifacts-invalid-spec");
        Files.createDirectories(sourceDir.resolve("raw"));
        Files.createDirectories(sourceDir.resolve("logs"));
        writeSummary(sourceDir.resolve("summary.json"), candidateSummaryModel());
        Files.writeString(sourceDir.resolve("raw/sample.json"), "{}",
                StandardCharsets.UTF_8);
        Files.writeString(sourceDir.resolve("logs/sample.log"), "ok\n",
                StandardCharsets.UTF_8);

        final ProcessResult publishResult = runPythonScript(
                "publish_jmh_history.py",
                "--source-dir", sourceDir.toString(),
                "--history-dir", historyDir.toString(),
                "--channel", "main",
                "--run-suffix", "invalid-spec");

        assertEquals(0, publishResult.exitCode(), publishResult.output());

        final List<String> invalidSpecs = List.of(
                "{}",
                """
                        {
                          "profile": "segment-index-pr-smoke",
                          "benchmarks": []
                        }
                        """,
                """
                        {
                          "profile": "segment-index-pr-smoke",
                          "benchmarks": [
                            { "label": "" }
                          ]
                        }
                        """);
        for (int i = 0; i < invalidSpecs.size(); i++) {
            final Path profileSpec = tempDir.resolve(
                    "invalid-profile-spec-" + i + ".json");
            Files.writeString(profileSpec, invalidSpecs.get(i),
                    StandardCharsets.UTF_8);

            final ProcessResult resolveResult = runPythonScript(
                    "resolve_jmh_history_baseline.py",
                    "--history-dir", historyDir.toString(),
                    "--profile", PROFILE,
                    "--profile-spec", profileSpec.toString(),
                    "--channel", "main");

            assertEquals(1, resolveResult.exitCode(), resolveResult.output());
        }
    }

    @Test
    void runProfileScriptSkipsBenchmarksMissingFromRepoRoot() throws Exception {
        assumePython3Available();
        final Path benchmarkRepo = tempDir.resolve("benchmark-repo");
        final Path benchmarkSourceRoot = benchmarkRepo.resolve("benchmarks")
                .resolve("src").resolve("main").resolve("java")
                .resolve("com").resolve("example");
        Files.createDirectories(benchmarkSourceRoot);
        Files.writeString(benchmarkSourceRoot.resolve("ExistingBenchmark.java"),
                """
                        package com.example;

                        public class ExistingBenchmark {
                        }
                        """,
                StandardCharsets.UTF_8);

        final Path profileSpec = tempDir.resolve("profile.json");
        Files.writeString(profileSpec, """
                {
                  "profile": "script-smoke",
                  "description": "script smoke",
                  "benchmarks": [
                    {
                      "label": "existing-benchmark",
                      "include": "com.example.ExistingBenchmark",
                      "args": [ "-wi", "1" ]
                    },
                    {
                      "label": "missing-benchmark",
                      "include": "com.example.MissingBenchmark",
                      "args": [ "-wi", "1" ]
                    }
                  ]
                }
                """, StandardCharsets.UTF_8);

        final Path outputDir = tempDir.resolve("profile-output");
        final Path runnerJar = createDummyJmhRunnerJar(
                tempDir.resolve("dummy-jmh-runner.jar"));

        final ProcessResult result = runPythonScript(
                "run_jmh_profile.py",
                "--repo-root", benchmarkRepo.toString(),
                "--profile", profileSpec.toString(),
                "--output-dir", outputDir.toString(),
                "--jar", runnerJar.toString(),
                "--skip-build");

        assertEquals(0, result.exitCode(), result.output());
        assertTrue(result.output().contains("Skipping benchmark 'missing-benchmark'"),
                result.output());

        final JsonNode summary = OBJECT_MAPPER
                .readTree(outputDir.resolve("summary.json").toFile());
        assertEquals(1, summary.path("benchmarks").size());
        assertEquals("existing-benchmark",
                summary.path("benchmarks").get(0).path("label").asText());
        assertEquals(1, summary.path("skippedBenchmarks").size());
        assertEquals("missing-benchmark",
                summary.path("skippedBenchmarks").get(0).path("label")
                        .asText());
        assertEquals("missing_benchmark_source",
                summary.path("skippedBenchmarks").get(0).path("reason")
                        .asText());
    }

    @Test
    void publishScriptStoresPrScopedHistoryWithoutOverwritingCanonicalMainPointer()
            throws Exception {
        assumePython3Available();
        final Path sourceDir = tempDir.resolve("candidate-pr-run");
        final Path historyDir = tempDir.resolve("perf-artifacts");
        final Path comparisonMarkdown = tempDir.resolve("pr-comparison.md");
        final Path comparisonJson = tempDir.resolve("pr-comparison.json");
        final Path mainMetadataJson = tempDir.resolve("publish-main-metadata.json");
        final Path prMetadataJson = tempDir.resolve("publish-pr-metadata.json");
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

        final ProcessResult publishMainResult = runPythonScript(
                "publish_jmh_history.py",
                "--source-dir", sourceDir.toString(),
                "--history-dir", historyDir.toString(),
                "--channel", "main",
                "--run-suffix", "main",
                "--metadata-out", mainMetadataJson.toString());

        assertEquals(0, publishMainResult.exitCode(), publishMainResult.output());

        final ProcessResult publishPrResult = runPythonScript(
                "publish_jmh_history.py",
                "--source-dir", sourceDir.toString(),
                "--history-dir", historyDir.toString(),
                "--channel", "pr-125",
                "--pr-number", "125",
                "--run-suffix", "pr",
                "--comparison-markdown", comparisonMarkdown.toString(),
                "--comparison-json", comparisonJson.toString(),
                "--metadata-out", prMetadataJson.toString());

        assertEquals(0, publishPrResult.exitCode(), publishPrResult.output());

        final Path canonicalPointer = historyDir.resolve("history")
                .resolve(PROFILE).resolve("latest-main.json");
        final Path prPointer = historyDir.resolve("history")
                .resolve(PROFILE).resolve("pull-requests")
                .resolve("pr-125").resolve("latest.json");
        assertTrue(Files.isRegularFile(canonicalPointer));
        assertTrue(Files.isRegularFile(prPointer));

        final JsonNode mainMetadata = OBJECT_MAPPER.readTree(mainMetadataJson.toFile());
        final JsonNode prMetadata = OBJECT_MAPPER.readTree(prMetadataJson.toFile());
        assertTrue(mainMetadata.path("prNumber").isNull());
        assertEquals("125", prMetadata.path("prNumber").asText());
        assertTrue(prMetadata.path("runPath").asText()
                .contains("/pull-requests/pr-125/2026/03/"));
        assertNotNull(prMetadata.path("comparisonMarkdownPath").asText(null));
        assertNotNull(prMetadata.path("comparisonJsonPath").asText(null));
        assertTrue(Files.isRegularFile(historyDir.resolve(
                prMetadata.path("comparisonMarkdownPath").asText())));
        assertTrue(Files.isRegularFile(historyDir.resolve(
                prMetadata.path("comparisonJsonPath").asText())));

        final JsonNode prPointerJson = OBJECT_MAPPER.readTree(prPointer.toFile());
        assertEquals("pr-125", prPointerJson.path("channel").asText());
        assertEquals("125", prPointerJson.path("prNumber").asText());
        assertTrue(prPointerJson.path("summaryPath").asText()
                .contains("/pull-requests/pr-125/"));

        final JsonNode index = OBJECT_MAPPER.readTree(historyDir.resolve("history")
                .resolve("index.json").toFile());
        assertEquals("main", index.path("profiles").path(PROFILE)
                .path("latestChannel").asText());
        assertEquals(mainMetadata.path("latestPointerPath").asText(),
                index.path("profiles").path(PROFILE)
                        .path("latestPointerPath").asText());
        assertEquals(prMetadata.path("latestPointerPath").asText(),
                index.path("profiles").path(PROFILE).path("pullRequests")
                        .path("125").path("latestPointerPath").asText());
        assertEquals("pr-125", index.path("profiles").path(PROFILE)
                .path("pullRequests").path("125").path("channel").asText());
        assertTrue(index.path("profiles").path(PROFILE).path("channels")
                .path("pr-125").isMissingNode());
        assertFalse(index.path("profiles").path(PROFILE)
                .path("latestPointerPath").asText()
                .contains("/pull-requests/"));
    }

    @Test
    void filterProfileScriptRetainsOnlyRequestedLabelsInSourceOrder()
            throws Exception {
        assumePython3Available();
        final Path profilePath = tempDir.resolve("profile.json");
        final Path outputPath = tempDir.resolve("profile-filtered.json");
        Files.writeString(profilePath, """
                {
                  "profile": "segment-index-pr-smoke",
                  "benchmarks": [
                    { "label": "alpha", "include": "Alpha", "args": ["-i", "1"] },
                    { "label": "beta", "include": "Beta", "args": ["-i", "1"] },
                    { "label": "gamma", "include": "Gamma", "args": ["-i", "1"] }
                  ]
                }
                """, StandardCharsets.UTF_8);

        final ProcessResult result = runPythonScript(
                "filter_jmh_profile.py",
                "--profile", profilePath.toString(),
                "--labels", "gamma,alpha",
                "--output", outputPath.toString());

        assertEquals(0, result.exitCode(), result.output());
        final JsonNode filtered = OBJECT_MAPPER.readTree(outputPath.toFile());
        assertEquals(List.of("alpha", "gamma"),
                benchmarkLabels(filtered));
    }

    @Test
    void aggregateSummariesScriptMedianMergesSelectedLabels() throws Exception {
        assumePython3Available();
        final Path baseSummary = tempDir.resolve("base-summary.json");
        final Path rerunOneSummary = tempDir.resolve("rerun-one-summary.json");
        final Path rerunTwoSummary = tempDir.resolve("rerun-two-summary.json");
        final Path outputSummary = tempDir.resolve("merged-summary.json");

        writeSummary(baseSummary, summary("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                List.of(
                        benchmark("segment-index-get-overlay",
                                BENCHMARK_GET_HIT,
                                metric(100D, 10D, "ops/s"),
                                Map.of("overlayProbe", metric(10D, 1D, "ops/s"))),
                        benchmark("segment-index-mixed-drain",
                                BENCHMARK_MIXED_PUT,
                                metric(210D, 5D, "ops/s"), Map.of()))));
        writeSummary(rerunOneSummary, summary("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                List.of(benchmark("segment-index-get-overlay",
                        BENCHMARK_GET_HIT,
                        metric(60D, 6D, "ops/s"),
                        Map.of("overlayProbe", metric(30D, 3D, "ops/s"))))));
        writeSummary(rerunTwoSummary, summary("cccccccccccccccccccccccccccccccccccccccc",
                List.of(benchmark("segment-index-get-overlay",
                        BENCHMARK_GET_HIT,
                        metric(80D, 8D, "ops/s"),
                        Map.of("overlayProbe",
                                metricWithLiteralScoreError(20D, "NaN",
                                        "ops/s"))))));

        final ProcessResult result = runPythonScript(
                "aggregate_jmh_profile_summaries.py",
                "--base-summary", baseSummary.toString(),
                "--supplemental-summary", rerunOneSummary.toString(),
                "--supplemental-summary", rerunTwoSummary.toString(),
                "--labels", "segment-index-get-overlay",
                "--output", outputSummary.toString());

        assertEquals(0, result.exitCode(), result.output());

        final JsonNode merged = OBJECT_MAPPER.readTree(outputSummary.toFile());
        final JsonNode overlay = benchmarkByLabel(merged,
                "segment-index-get-overlay");
        final JsonNode overlayRow = overlay.path("normalized").path("results")
                .get(0);
        assertEquals(80D,
                overlayRow.path("primaryMetric").path("score").asDouble());
        assertEquals(8D,
                overlayRow.path("primaryMetric").path("scoreError")
                        .asDouble());
        assertEquals(20D, overlayRow.path("secondaryMetrics")
                .path("overlayProbe").path("score").asDouble());
        assertEquals(2D, overlayRow.path("secondaryMetrics")
                .path("overlayProbe").path("scoreError").asDouble());
        assertEquals(210D, benchmarkByLabel(merged, "segment-index-mixed-drain")
                .path("normalized").path("results").get(0)
                .path("primaryMetric").path("score").asDouble());
        assertEquals(3,
                merged.path("aggregation").path("candidateRunCount").asInt());
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

    private boolean hasMetricWithDisplayName(final JsonNode comparison,
            final String displayName) {
        return firstMetricByDisplayName(comparison, displayName) != null;
    }

    private JsonNode firstMetricByDisplayName(final JsonNode comparison,
            final String displayName) {
        for (final JsonNode metric : comparison.path("metrics")) {
            if (displayName.equals(metric.path("displayName").asText())) {
                return metric;
            }
        }
        return null;
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
                                BENCHMARK_GET_HIT,
                                metric(100D, 10D, "ops/s"),
                                Map.of(
                                        "overlayProbe", metric(10D, 1D,
                                                "ops/s"),
                                        "diag_fileBytesDelta", metric(1000D, 50D,
                                                "ops/s"))),
                        benchmark("segment-index-get-persisted",
                                BENCHMARK_GET_MISS,
                                metricWithLiteralScoreError(90D, "NaN",
                                        "ops/s"),
                                Map.of())));
    }

    private Map<String, Object> candidateSummaryModel() {
        return summary("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                List.of(
                        benchmark("segment-index-get-overlay",
                                BENCHMARK_GET_HIT,
                                metric(80D, 8D, "ops/s"),
                                Map.of(
                                        "overlayProbe", metric(12D, 1.2D,
                                                "ops/s"),
                                        "diag_fileBytesDelta", metric(800D, 40D,
                                                "ops/s"))),
                        benchmark("segment-index-mixed-drain",
                                BENCHMARK_MIXED_PUT,
                                metric(210D, 21D, "ops/s"), Map.of())));
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
            final String benchmarkName, final Map<String, Object> primaryMetric,
            final Map<String, Object> secondaryMetrics) {
        return Map.of(
                "label", label,
                "normalized", Map.of(
                        "results", List.of(Map.of(
                                "benchmark", benchmarkName,
                                "primaryMetric", new LinkedHashMap<>(
                                        primaryMetric),
                                "secondaryMetrics",
                                new LinkedHashMap<>(secondaryMetrics)))));
    }

    private Map<String, Object> metric(final double score, final String unit) {
        return metric(score, null, unit);
    }

    private Map<String, Object> metric(final double score,
            final Double scoreError, final String unit) {
        final Map<String, Object> metric = new LinkedHashMap<>();
        metric.put("score", Double.valueOf(score));
        metric.put("scoreUnit", unit);
        if (scoreError != null) {
            metric.put("scoreError", scoreError);
        }
        return metric;
    }

    private Map<String, Object> metricWithLiteralScoreError(final double score,
            final String scoreError, final String unit) {
        final Map<String, Object> metric = new LinkedHashMap<>();
        metric.put("score", Double.valueOf(score));
        metric.put("scoreUnit", unit);
        metric.put("scoreError", scoreError);
        return metric;
    }

    private List<String> benchmarkLabels(final JsonNode profile) {
        final List<String> labels = new ArrayList<>();
        for (final JsonNode benchmark : profile.path("benchmarks")) {
            labels.add(benchmark.path("label").asText());
        }
        return labels;
    }

    private JsonNode benchmarkByLabel(final JsonNode summary, final String label) {
        for (final JsonNode benchmark : summary.path("benchmarks")) {
            if (label.equals(benchmark.path("label").asText())) {
                return benchmark;
            }
        }
        return null;
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

    private Path createDummyJmhRunnerJar(final Path jarPath) throws Exception {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assumptions.assumeTrue(compiler != null,
                "JDK compiler is required for dummy JMH runner smoke tests");

        final Path sourceDir = tempDir.resolve("dummy-jmh-src");
        final Path classesDir = tempDir.resolve("dummy-jmh-classes");
        Files.createDirectories(sourceDir);
        Files.createDirectories(classesDir);

        final Path sourceFile = sourceDir.resolve("DummyJmhMain.java");
        Files.writeString(sourceFile, """
                import java.nio.charset.StandardCharsets;
                import java.nio.file.Files;
                import java.nio.file.Path;

                public final class DummyJmhMain {

                    private DummyJmhMain() {
                    }

                    public static void main(final String[] args)
                            throws Exception {
                        if (args.length == 0) {
                            throw new IllegalArgumentException(
                                    "Missing benchmark include");
                        }
                        final String benchmark = args[0];
                        Path resultPath = null;
                        for (int index = 0; index < args.length - 1; index++) {
                            if ("-rff".equals(args[index])) {
                                resultPath = Path.of(args[index + 1]);
                                break;
                            }
                        }
                        if (resultPath == null) {
                            throw new IllegalArgumentException(
                                    "Missing -rff argument");
                        }
                        Files.createDirectories(resultPath.getParent());
                        final String payload = "[{"
                                + "\\"benchmark\\":\\"" + benchmark
                                + ".run\\","
                                + "\\"mode\\":\\"thrpt\\","
                                + "\\"threads\\":1,"
                                + "\\"primaryMetric\\":{"
                                + "\\"score\\":1.0,"
                                + "\\"scoreUnit\\":\\"ops/s\\""
                                + "}"
                                + "}]";
                        Files.writeString(resultPath, payload,
                                StandardCharsets.UTF_8);
                    }
                }
                """, StandardCharsets.UTF_8);

        final int compileExit = compiler.run(null, null, null, "-d",
                classesDir.toString(), sourceFile.toString());
        assertEquals(0, compileExit, "Failed to compile dummy JMH runner");

        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION,
                "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS,
                "DummyJmhMain");

        try (JarOutputStream jarOutput = new JarOutputStream(
                Files.newOutputStream(jarPath), manifest)) {
            final Path classFile = classesDir.resolve("DummyJmhMain.class");
            jarOutput.putNextEntry(new JarEntry("DummyJmhMain.class"));
            jarOutput.write(Files.readAllBytes(classFile));
            jarOutput.closeEntry();
        }
        return jarPath;
    }

    private record ProcessResult(int exitCode, String output) {
    }
}
