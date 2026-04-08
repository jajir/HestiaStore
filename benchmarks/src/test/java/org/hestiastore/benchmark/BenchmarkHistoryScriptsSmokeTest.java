package org.hestiastore.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
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
        assertTrue(hasMetricWithStatus(comparison, "segment-index-get-live:getHit:liveProbe",
                "better"));
        assertTrue(hasMetricWithStatus(comparison,
                "segment-index-mixed-drain:putWorkload", "new"));
        assertTrue(hasMetricWithStatus(comparison,
                "segment-index-get-persisted:getMiss", "removed"));

        final String markdown = Files.readString(markdownOut,
                StandardCharsets.UTF_8);
        assertTrue(markdown.contains("segment-index-get-live:getHit"));
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
                    { "label": "segment-index-get-live" },
                    { "label": "segment-index-get-persisted" },
                    { "label": "segment-index-hot-route-put" }
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
    void syncBenchmarkDocsScriptCopiesLatestArtifactsToCanonicalNamesAndRemovesObsoleteFiles()
            throws Exception {
        assumePython3Available();
        final Path sourceRoot = tempDir.resolve("generated");
        final Path targetRoot = tempDir.resolve("site");
        final Path sourceDocs = sourceRoot.resolve("docs").resolve("why-hestiastore");
        final Path sourceImages = sourceRoot.resolve("docs").resolve("images");
        final Path targetDocs = targetRoot.resolve("docs").resolve("why-hestiastore");
        final Path targetImages = targetRoot.resolve("docs").resolve("images");
        Files.createDirectories(sourceDocs);
        Files.createDirectories(sourceImages);
        Files.createDirectories(targetDocs);
        Files.createDirectories(targetImages);

        writeText(sourceDocs.resolve("out-write.md"),
                "# old write\n![chart](../images/out-write.svg)\n",
                1_000L);
        writeText(sourceDocs.resolve("out-write-single-thread.md"), """
                # latest single-thread write

                ![chart](../images/out-write-single-thread.svg)
                ![percentiles](../images/out-write-single-thread-percentiles.svg)
                """, 2_000L);
        writeText(sourceImages.resolve("out-write.svg"), "<svg>old-write</svg>\n",
                1_000L);
        writeText(sourceImages.resolve("out-write-single-thread.svg"),
                "<svg>latest-write</svg>\n", 2_000L);
        writeText(sourceImages.resolve("out-write-percentiles.svg"),
                "<svg>old-write-percentiles</svg>\n", 1_000L);
        writeText(sourceImages.resolve("out-write-single-thread-percentiles.svg"),
                "<svg>latest-write-percentiles</svg>\n", 2_000L);

        writeText(sourceDocs.resolve("out-read-single-thread.md"), """
                # latest single-thread read

                ![chart](../images/out-read-single-thread.svg)
                ![percentiles](../images/out-read-single-thread-percentiles.svg)
                """, 2_000L);
        writeText(sourceImages.resolve("out-read-single-thread.svg"),
                "<svg>latest-read</svg>\n", 2_000L);
        writeText(sourceImages.resolve("out-read-single-thread-percentiles.svg"),
                "<svg>latest-read-percentiles</svg>\n", 2_000L);

        writeText(sourceDocs.resolve("out-sequential-read.md"), """
                # latest sequential read

                ![chart](../images/out-sequential-read.svg)
                ![percentiles](../images/out-sequential-read-percentiles.svg)
                """, 2_000L);
        writeText(sourceImages.resolve("out-sequential-read.svg"),
                "<svg>latest-sequential</svg>\n", 2_000L);
        writeText(sourceImages.resolve("out-sequential-read-percentiles.svg"),
                "<svg>latest-sequential-percentiles</svg>\n", 2_000L);

        writeText(sourceDocs.resolve("out-write-multi-thread.md"), """
                # latest multi-thread write

                ![chart](../images/out-write-multi-thread.svg)
                ![percentiles](../images/out-write-multi-thread-percentiles.svg)
                """, 2_000L);
        writeText(sourceImages.resolve("out-write-multi-thread.svg"),
                "<svg>latest-multi-write</svg>\n", 2_000L);
        writeText(sourceImages.resolve("out-write-multi-thread-percentiles.svg"),
                "<svg>latest-multi-write-percentiles</svg>\n", 2_000L);

        writeText(sourceDocs.resolve("out-read-multi-thread.md"), """
                # latest multi-thread read

                ![chart](../images/out-read-multi-thread.svg)
                ![percentiles](../images/out-read-multi-thread-percentiles.svg)
                """, 2_000L);
        writeText(sourceImages.resolve("out-read-multi-thread.svg"),
                "<svg>latest-multi-read</svg>\n", 2_000L);
        writeText(sourceImages.resolve("out-read-multi-thread-percentiles.svg"),
                "<svg>latest-multi-read-percentiles</svg>\n", 2_000L);

        writeText(targetDocs.resolve("out-write-single-thread.md"), "obsolete\n",
                500L);
        writeText(targetDocs.resolve("out-read-single-thread.md"), "obsolete\n",
                500L);
        writeText(targetDocs.resolve("out-sequential-read.md"), "obsolete\n",
                500L);
        writeText(targetDocs.resolve("out-write-multi-thread.md"), "obsolete\n",
                500L);
        writeText(targetDocs.resolve("out-read-multi-thread.md"), "obsolete\n",
                500L);
        writeText(targetImages.resolve("out-write-single-thread.svg"), "obsolete\n",
                500L);
        writeText(targetImages.resolve("out-write-single-thread-percentiles.svg"),
                "obsolete\n", 500L);
        writeText(targetImages.resolve("out-read-single-thread.svg"), "obsolete\n",
                500L);
        writeText(targetImages.resolve("out-read-single-thread-percentiles.svg"),
                "obsolete\n", 500L);
        writeText(targetImages.resolve("out-sequential-read.svg"), "obsolete\n",
                500L);
        writeText(targetImages.resolve("out-sequential-read-percentiles.svg"),
                "obsolete\n", 500L);
        writeText(targetImages.resolve("out-write-multi-thread.svg"), "obsolete\n",
                500L);
        writeText(targetImages.resolve("out-write-multi-thread-percentiles.svg"),
                "obsolete\n", 500L);
        writeText(targetImages.resolve("out-read-multi-thread.svg"), "obsolete\n",
                500L);
        writeText(targetImages.resolve("out-read-multi-thread-percentiles.svg"),
                "obsolete\n", 500L);

        final ProcessResult result = runPythonScript(
                "sync_benchmark_docs.py",
                "--source-root", sourceRoot.toString(),
                "--target-root", targetRoot.toString());

        assertEquals(0, result.exitCode(), result.output());

        final String writeMarkdown = Files.readString(targetDocs.resolve("out-write.md"),
                StandardCharsets.UTF_8);
        assertTrue(writeMarkdown.contains("# latest single-thread write"));
        assertTrue(writeMarkdown.contains("../images/out-write.svg"));
        assertTrue(writeMarkdown.contains("../images/out-write-percentiles.svg"));
        assertFalse(writeMarkdown.contains("out-write-single-thread.svg"));
        assertEquals("<svg>latest-write</svg>\n",
                Files.readString(targetImages.resolve("out-write.svg"),
                        StandardCharsets.UTF_8));
        assertEquals("<svg>latest-write-percentiles</svg>\n",
                Files.readString(targetImages.resolve("out-write-percentiles.svg"),
                        StandardCharsets.UTF_8));
        assertEquals("<svg>latest-multi-read</svg>\n",
                Files.readString(targetImages.resolve("out-multithread-read.svg"),
                        StandardCharsets.UTF_8));
        assertEquals("<svg>latest-multi-read-percentiles</svg>\n",
                Files.readString(targetImages.resolve(
                        "out-multithread-read-percentiles.svg"),
                        StandardCharsets.UTF_8));

        assertFalse(Files.exists(targetDocs.resolve("out-write-single-thread.md")));
        assertFalse(Files.exists(targetDocs.resolve("out-read-single-thread.md")));
        assertFalse(Files.exists(targetDocs.resolve("out-sequential-read.md")));
        assertFalse(Files.exists(targetDocs.resolve("out-write-multi-thread.md")));
        assertFalse(Files.exists(targetDocs.resolve("out-read-multi-thread.md")));
        assertFalse(Files.exists(targetImages.resolve("out-write-single-thread.svg")));
        assertFalse(Files.exists(
                targetImages.resolve("out-write-single-thread-percentiles.svg")));
        assertFalse(Files.exists(targetImages.resolve("out-read-single-thread.svg")));
        assertFalse(Files.exists(
                targetImages.resolve("out-read-single-thread-percentiles.svg")));
        assertFalse(Files.exists(targetImages.resolve("out-sequential-read.svg")));
        assertFalse(Files.exists(
                targetImages.resolve("out-sequential-read-percentiles.svg")));
        assertFalse(Files.exists(targetImages.resolve("out-write-multi-thread.svg")));
        assertFalse(Files.exists(
                targetImages.resolve("out-write-multi-thread-percentiles.svg")));
        assertFalse(Files.exists(targetImages.resolve("out-read-multi-thread.svg")));
        assertFalse(Files.exists(
                targetImages.resolve("out-read-multi-thread-percentiles.svg")));
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

    private void writeText(final Path path, final String value,
            final long modifiedMillis) throws IOException {
        path.getParent().toFile().mkdirs();
        Files.writeString(path, value, StandardCharsets.UTF_8);
        Files.setLastModifiedTime(path, FileTime.fromMillis(modifiedMillis));
    }

    private Map<String, Object> baselineSummaryModel() {
        return summary("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                List.of(
                        benchmark("segment-index-get-live",
                                BENCHMARK_GET_HIT, 100D,
                                Map.of("liveProbe", metric(10D, "ops/s"))),
                        benchmark("segment-index-get-persisted",
                                BENCHMARK_GET_MISS, 90D, Map.of())));
    }

    private Map<String, Object> candidateSummaryModel() {
        return summary("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                List.of(
                        benchmark("segment-index-get-live",
                                BENCHMARK_GET_HIT, 80D,
                                Map.of("liveProbe", metric(12D, "ops/s"))),
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
