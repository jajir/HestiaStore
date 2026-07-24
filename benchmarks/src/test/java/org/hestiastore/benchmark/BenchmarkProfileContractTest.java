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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class BenchmarkProfileContractTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String LEGACY_BACKGROUND_MAINTENANCE_METHOD = "withSegmentMaintenanceAutoEnabled";
    private static final Set<String> REQUIRED_PR_SEGMENT_INDEX_LABELS = Set.of(
            "segment-index-get-persisted",
            "segment-index-get-live",
            "segment-index-get-multisegment-hot",
            "segment-index-persisted-mutation",
            "segment-index-persisted-mutation-concurrent",
            "segment-index-hot-route-put",
            "segment-index-mixed-drain",
            "segment-index-mixed-split-heavy");
    private static final Set<String> REQUIRED_NIGHTLY_SEGMENT_INDEX_LABELS = Set.of(
            "segment-index-get-persisted",
            "segment-index-get-live",
            "segment-index-get-multisegment-hot",
            "segment-index-get-multisegment-cold",
            "segment-index-persisted-mutation",
            "segment-index-persisted-mutation-concurrent",
            "segment-index-lifecycle",
            "segment-index-hot-route-put",
            "segment-index-mixed-drain",
            "segment-index-mixed-split-heavy");
    private static final Set<String> REQUIRED_NIGHTLY_DISKIO_LABELS = Set.of(
            "diskio-sequential-write-1k",
            "diskio-sequential-write-4k",
            "diskio-sequential-write-32k",
            "diskio-sequential-read-1k",
            "diskio-sequential-read-4k",
            "diskio-sequential-read-32k");
    private static final Set<String> REQUIRED_CONTEXT_LOGGING_LABELS = Set.of(
            "segment-index-live-get-context-disabled",
            "segment-index-live-get-context-enabled",
            "segment-index-hot-put-context-disabled",
            "segment-index-hot-put-context-enabled");

    @Test
    void allBenchmarkProfilesUseUniqueLabelsAndResolvableBenchmarkClasses()
            throws Exception {
        for (final BenchmarkProfile profile : loadProfiles()) {
            final Set<String> labels = new LinkedHashSet<>();
            for (final BenchmarkEntry benchmark : profile.benchmarks()) {
                assertTrue(labels.add(benchmark.label()),
                        () -> "Duplicate benchmark label in profile "
                                + profile.profile() + ": " + benchmark.label());
                assertFalse(benchmark.label().isBlank(),
                        () -> "Blank benchmark label in profile "
                                + profile.profile());
                assertFalse(benchmark.include().isBlank(),
                        () -> "Blank include in profile " + profile.profile()
                                + " for label " + benchmark.label());
                assertFalse(benchmark.args().isEmpty(),
                        () -> "Missing args in profile " + profile.profile()
                                + " for label " + benchmark.label());
                assertNotNull(loadBenchmarkClass(benchmark.include()),
                        () -> "Unable to load benchmark class "
                                + benchmark.include());
                assertTrue(
                        Files.isRegularFile(sourcePathForInclude(
                                benchmark.include())),
                        () -> "Missing benchmark source file for "
                                + benchmark.include());
            }
        }
    }

    @Test
    void canonicalSegmentIndexProfilesCoverRequiredScenarios() throws Exception {
        final Map<String, BenchmarkProfile> profilesByName = new HashMap<>();
        for (final BenchmarkProfile profile : loadProfiles()) {
            profilesByName.put(profile.profile(), profile);
        }

        assertCanonicalPrSegmentIndexProfile(
                profilesByName.get("segment-index-pr-smoke"));
        assertCanonicalNightlySegmentIndexProfile(
                profilesByName.get("segment-index-nightly"));
        assertCanonicalDiskIoNightlyProfile(profilesByName.get("diskio-nightly"));
        assertContextLoggingProfile(
                profilesByName.get("segment-index-context-logging"));
    }

    @Test
    void segmentIndexBenchmarkSourcesUseBackgroundMaintenanceBuilderName()
            throws Exception {
        try (Stream<Path> files = Files.walk(segmentIndexBenchmarkSourceRoot())) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .sorted(Comparator.naturalOrder()).forEach(path -> {
                        final String source = readUtf8(path);
                        assertFalse(source.contains(
                                LEGACY_BACKGROUND_MAINTENANCE_METHOD),
                                () -> "Legacy builder method remains in "
                                        + path);
                    });
        }
    }

    private void assertCanonicalPrSegmentIndexProfile(
            final BenchmarkProfile profile) {
        assertNotNull(profile, "Missing canonical segment-index profile");
        assertCanonicalProfileMetadata(profile);
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_PR_SEGMENT_INDEX_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("segment-index-get-persisted"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("contextLogging", "false", "readPathMode",
                        "persisted"));
        assertEntry(byLabel.get("segment-index-get-live"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("contextLogging", "false", "readPathMode", "live"));
        assertEntry(byLabel.get("segment-index-get-multisegment-hot"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMultiSegmentGetBenchmark",
                Map.of("workingSetMode", "hot"));
        assertEntry(byLabel.get("segment-index-persisted-mutation"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark",
                Map.of("walMode", "sync"));
        assertThreadCount(byLabel.get("segment-index-persisted-mutation"), 1);
        assertEntry(byLabel.get("segment-index-persisted-mutation-concurrent"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark",
                Map.of("walMode", "sync"));
        assertThreadCount(
                byLabel.get("segment-index-persisted-mutation-concurrent"),
                16);
        assertEntry(byLabel.get("segment-index-hot-route-put"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexHotRoutePutBenchmark",
                Map.of("contextLogging", "false"));
        assertEntry(byLabel.get("segment-index-mixed-drain"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark",
                Map.of("workloadMode", "drainOnly"));
        assertEntry(byLabel.get("segment-index-mixed-split-heavy"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark",
                Map.of("workloadMode", "splitHeavy"));
    }

    private void assertCanonicalNightlySegmentIndexProfile(
            final BenchmarkProfile profile) {
        assertNotNull(profile, "Missing canonical segment-index profile");
        assertCanonicalProfileMetadata(profile);
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_NIGHTLY_SEGMENT_INDEX_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("segment-index-get-persisted"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("contextLogging", "false", "readPathMode",
                        "persisted"));
        assertEntry(byLabel.get("segment-index-get-live"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("contextLogging", "false", "readPathMode", "live"));
        assertEntry(byLabel.get("segment-index-get-multisegment-hot"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMultiSegmentGetBenchmark",
                Map.of("workingSetMode", "hot"));
        assertEntry(byLabel.get("segment-index-get-multisegment-cold"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMultiSegmentGetBenchmark",
                Map.of("workingSetMode", "cold"));
        assertEntry(byLabel.get("segment-index-persisted-mutation"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark",
                Map.of("walMode", "sync"));
        assertThreadCount(byLabel.get("segment-index-persisted-mutation"), 1);
        assertEntry(byLabel.get("segment-index-persisted-mutation-concurrent"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark",
                Map.of("walMode", "sync"));
        assertThreadCount(
                byLabel.get("segment-index-persisted-mutation-concurrent"),
                16);
        assertEntry(byLabel.get("segment-index-lifecycle"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexLifecycleBenchmark",
                Map.of("walMode", "sync"));
        assertEntry(byLabel.get("segment-index-hot-route-put"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexHotRoutePutBenchmark",
                Map.of("contextLogging", "false"));
        assertEntry(byLabel.get("segment-index-mixed-drain"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark",
                Map.of("workloadMode", "drainOnly"));
        assertEntry(byLabel.get("segment-index-mixed-split-heavy"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark",
                Map.of("workloadMode", "splitHeavy"));
    }

    private void assertCanonicalDiskIoNightlyProfile(
            final BenchmarkProfile profile) {
        assertNotNull(profile, "Missing canonical diskio nightly profile");
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_NIGHTLY_DISKIO_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("diskio-sequential-write-1k"),
                "org.hestiastore.benchmark.diskio.write.sequential.SequentialFileWritingBenchmark",
                Map.of("diskIoBufferSizeBytes", "1024"));
        assertEntry(byLabel.get("diskio-sequential-write-4k"),
                "org.hestiastore.benchmark.diskio.write.sequential.SequentialFileWritingBenchmark",
                Map.of("diskIoBufferSizeBytes", "4096"));
        assertEntry(byLabel.get("diskio-sequential-write-32k"),
                "org.hestiastore.benchmark.diskio.write.sequential.SequentialFileWritingBenchmark",
                Map.of("diskIoBufferSizeBytes", "32768"));
        assertEntry(byLabel.get("diskio-sequential-read-1k"),
                "org.hestiastore.benchmark.diskio.read.sequential.SequentialFileReadingBenchmark",
                Map.of("diskIoBufferSizeBytes", "1024"));
        assertEntry(byLabel.get("diskio-sequential-read-4k"),
                "org.hestiastore.benchmark.diskio.read.sequential.SequentialFileReadingBenchmark",
                Map.of("diskIoBufferSizeBytes", "4096"));
        assertEntry(byLabel.get("diskio-sequential-read-32k"),
                "org.hestiastore.benchmark.diskio.read.sequential.SequentialFileReadingBenchmark",
                Map.of("diskIoBufferSizeBytes", "32768"));
    }

    private void assertContextLoggingProfile(final BenchmarkProfile profile) {
        assertNotNull(profile, "Missing context-logging profile");
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_CONTEXT_LOGGING_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertContextLoggingEntry(
                byLabel.get("segment-index-live-get-context-disabled"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                "false", "live");
        assertContextLoggingEntry(
                byLabel.get("segment-index-live-get-context-enabled"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                "true", "live");
        assertContextLoggingEntry(
                byLabel.get("segment-index-hot-put-context-disabled"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexHotRoutePutBenchmark",
                "false", null);
        assertContextLoggingEntry(
                byLabel.get("segment-index-hot-put-context-enabled"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexHotRoutePutBenchmark",
                "true", null);
    }

    private void assertContextLoggingEntry(final BenchmarkEntry entry,
            final String include, final String contextLogging,
            final String readPathMode) {
        final Map<String, String> params = new LinkedHashMap<>();
        params.put("contextLogging", contextLogging);
        if (readPathMode != null) {
            params.put("readPathMode", readPathMode);
        }
        assertEntry(entry, include, params);
        assertOptionValue(entry, "-f", "3");
        assertOptionValue(entry, "-prof", "gc");
    }

    private void assertEntry(final BenchmarkEntry entry, final String include,
            final Map<String, String> requiredParams) {
        assertNotNull(entry, "Missing benchmark entry for " + include);
        assertEquals(include, entry.include(),
                () -> "Unexpected include for label " + entry.label());
        final Map<String, List<String>> params = parseParams(entry.args());
        for (final Map.Entry<String, String> required : requiredParams
                .entrySet()) {
            final List<String> values = params.get(required.getKey());
            assertNotNull(values,
                    () -> "Missing parameter " + required.getKey() + " for "
                            + entry.label());
            assertTrue(values.contains(required.getValue()),
                    () -> "Missing value " + required.getValue() + " for "
                            + required.getKey() + " in " + entry.label());
        }
    }

    private void assertCanonicalProfileMetadata(
            final BenchmarkProfile profile) {
        assertNotNull(profile.supportSources(),
                () -> "Missing support sources for " + profile.profile());
        assertFalse(profile.supportSources().isEmpty(),
                () -> "Empty support sources for " + profile.profile());
        for (final String supportSource : profile.supportSources()) {
            assertTrue(Files.isRegularFile(repoRoot().resolve(supportSource)),
                    () -> "Missing benchmark support source "
                            + supportSource);
        }
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            assertNotNull(benchmark.expectedBenchmarks(),
                    () -> "Missing expected benchmark methods for "
                            + benchmark.label());
            assertFalse(benchmark.expectedBenchmarks().isEmpty(),
                    () -> "Empty expected benchmark methods for "
                            + benchmark.label());
            assertEquals(benchmark.expectedBenchmarks().size(),
                    new LinkedHashSet<>(benchmark.expectedBenchmarks()).size(),
                    () -> "Duplicate expected benchmark methods for "
                            + benchmark.label());
            assertTrue(benchmark.expectedResultCount() >= benchmark
                    .expectedBenchmarks().size(),
                    () -> "Invalid expected result count for "
                            + benchmark.label());
            for (final String expectedBenchmark : benchmark
                    .expectedBenchmarks()) {
                assertTrue(
                        expectedBenchmark.startsWith(benchmark.include() + "."),
                        () -> "Unexpected benchmark method "
                                + expectedBenchmark + " for "
                                + benchmark.label());
            }
        }
    }

    private void assertThreadCount(final BenchmarkEntry entry,
            final int expectedThreadCount) {
        assertOptionValue(entry, "-t", Integer.toString(expectedThreadCount));
    }

    private void assertOptionValue(final BenchmarkEntry entry,
            final String option, final String expectedValue) {
        final List<String> args = entry.args();
        final int optionIndex = args.indexOf(option);
        assertTrue(optionIndex >= 0 && optionIndex + 1 < args.size(),
                () -> "Missing " + option + " for " + entry.label());
        assertEquals(expectedValue, args.get(optionIndex + 1),
                () -> "Unexpected " + option + " for " + entry.label());
    }

    private List<BenchmarkProfile> loadProfiles() throws Exception {
        final List<BenchmarkProfile> profiles = new ArrayList<>();
        try (Stream<Path> files = Files.list(profileRoot())) {
            for (final Path file : files.filter(path -> path.toString()
                    .endsWith(".json")).sorted(Comparator.naturalOrder())
                    .toList()) {
                profiles.add(OBJECT_MAPPER.readValue(file.toFile(),
                        BenchmarkProfile.class));
            }
        }
        return profiles;
    }

    private Map<String, List<String>> parseParams(final List<String> args) {
        final Map<String, List<String>> params = new LinkedHashMap<>();
        for (int index = 0; index < args.size() - 1; index++) {
            if (!"-p".equals(args.get(index))) {
                continue;
            }
            final String raw = args.get(index + 1);
            final int separator = raw.indexOf('=');
            if (separator <= 0 || separator >= raw.length() - 1) {
                continue;
            }
            final String key = raw.substring(0, separator);
            final String value = raw.substring(separator + 1);
            params.computeIfAbsent(key, ignored -> new ArrayList<>())
                    .add(value);
        }
        return params;
    }

    private Class<?> loadBenchmarkClass(final String include)
            throws ClassNotFoundException {
        return Class.forName(include);
    }

    private Path sourcePathForInclude(final String include) {
        return sourceRoot()
                .resolve(include.replace('.', '/') + ".java");
    }

    private Path segmentIndexBenchmarkSourceRoot() {
        return sourceRoot().resolve("org/hestiastore/benchmark/segmentindex");
    }

    private Path profileRoot() {
        final Path current = Path.of("").toAbsolutePath().normalize();
        final Path moduleProfiles = current.resolve("profiles");
        if (Files.isDirectory(moduleProfiles)) {
            return moduleProfiles;
        }
        return current.resolve("benchmarks").resolve("profiles");
    }

    private Path sourceRoot() {
        final Path current = Path.of("").toAbsolutePath().normalize();
        final Path moduleSources = current.resolve("src/main/java");
        if (Files.isDirectory(moduleSources)) {
            return moduleSources;
        }
        return current.resolve("benchmarks").resolve("src/main/java");
    }

    private Path repoRoot() {
        final Path sourceRoot = sourceRoot();
        return sourceRoot.getParent().getParent().getParent().getParent();
    }

    private String readUtf8(final Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to read " + path, e);
        }
    }

    record BenchmarkProfile(String profile, String description,
            List<String> supportSources,
            List<BenchmarkEntry> benchmarks) {
    }

    record BenchmarkEntry(String label, String include,
            List<String> expectedBenchmarks, int expectedResultCount,
            List<String> args) {
    }
}
