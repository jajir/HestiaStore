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
            "segment-index-get-overlay",
            "segment-index-get-multisegment-hot",
            "segment-index-persisted-mutation",
            "segment-index-hot-partition-put",
            "segment-index-mixed-drain",
            "segment-index-mixed-split-heavy");
    private static final Set<String> REQUIRED_NIGHTLY_SEGMENT_INDEX_LABELS = Set.of(
            "segment-index-get-persisted",
            "segment-index-get-overlay",
            "segment-index-get-multisegment-hot",
            "segment-index-get-multisegment-cold",
            "segment-index-persisted-mutation",
            "segment-index-lifecycle",
            "segment-index-hot-partition-put",
            "segment-index-mixed-drain",
            "segment-index-mixed-split-heavy");
    private static final Set<String> REQUIRED_PR_STORAGE_CORE_LABELS = Set.of(
            "sorted-data-diff-key-read", "single-chunk-entry-write");
    private static final Set<String> REQUIRED_NIGHTLY_STORAGE_CORE_LABELS = Set
            .of("sorted-data-diff-key-read-compact",
                    "sorted-data-diff-key-read-large",
                    "single-chunk-entry-write-compact",
                    "single-chunk-entry-write-large");

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
    void canonicalBenchmarkProfilesCoverRequiredScenarios() throws Exception {
        final Map<String, BenchmarkProfile> profilesByName = new HashMap<>();
        for (final BenchmarkProfile profile : loadProfiles()) {
            profilesByName.put(profile.profile(), profile);
        }

        assertCanonicalPrSegmentIndexProfile(
                profilesByName.get("segment-index-pr-smoke"));
        assertCanonicalNightlySegmentIndexProfile(
                profilesByName.get("segment-index-nightly"));
        assertCanonicalPrStorageCoreProfile(
                profilesByName.get("storage-core-pr-smoke"));
        assertCanonicalNightlyStorageCoreProfile(
                profilesByName.get("storage-core-nightly"));
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
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_PR_SEGMENT_INDEX_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("segment-index-get-persisted"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("readPathMode", "persisted"));
        assertEntry(byLabel.get("segment-index-get-overlay"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("readPathMode", "overlay"));
        assertEntry(byLabel.get("segment-index-get-multisegment-hot"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMultiSegmentGetBenchmark",
                Map.of("workingSetMode", "hot"));
        assertEntry(byLabel.get("segment-index-persisted-mutation"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark",
                Map.of("walMode", "sync"));
        assertEntry(byLabel.get("segment-index-hot-partition-put"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexHotPartitionPutBenchmark",
                Map.of());
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
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_NIGHTLY_SEGMENT_INDEX_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("segment-index-get-persisted"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("readPathMode", "persisted"));
        assertEntry(byLabel.get("segment-index-get-overlay"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexGetBenchmark",
                Map.of("readPathMode", "overlay"));
        assertEntry(byLabel.get("segment-index-get-multisegment-hot"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMultiSegmentGetBenchmark",
                Map.of("workingSetMode", "hot"));
        assertEntry(byLabel.get("segment-index-get-multisegment-cold"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMultiSegmentGetBenchmark",
                Map.of("workingSetMode", "cold"));
        assertEntry(byLabel.get("segment-index-persisted-mutation"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark",
                Map.of("walMode", "sync"));
        assertEntry(byLabel.get("segment-index-lifecycle"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexLifecycleBenchmark",
                Map.of("walMode", "sync"));
        assertEntry(byLabel.get("segment-index-hot-partition-put"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexHotPartitionPutBenchmark",
                Map.of());
        assertEntry(byLabel.get("segment-index-mixed-drain"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark",
                Map.of("workloadMode", "drainOnly"));
        assertEntry(byLabel.get("segment-index-mixed-split-heavy"),
                "org.hestiastore.benchmark.segmentindex.SegmentIndexMixedDrainBenchmark",
                Map.of("workloadMode", "splitHeavy"));
    }

    private void assertCanonicalPrStorageCoreProfile(
            final BenchmarkProfile profile) {
        assertNotNull(profile, "Missing canonical storage profile");
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_PR_STORAGE_CORE_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("sorted-data-diff-key-read"),
                "org.hestiastore.benchmark.sorteddatafile.DiffKeyReaderBenchmark",
                Map.of("entryCount", "8192", "keyLength", "48", "valueLength",
                        "64"));
        assertEntry(byLabel.get("single-chunk-entry-write"),
                "org.hestiastore.benchmark.chunkentryfile.SingleChunkEntryWriterBenchmark",
                Map.of("entriesPerChunk", "256", "valueLength", "64"));
    }

    private void assertCanonicalNightlyStorageCoreProfile(
            final BenchmarkProfile profile) {
        assertNotNull(profile, "Missing canonical storage profile");
        final Map<String, BenchmarkEntry> byLabel = new LinkedHashMap<>();
        for (final BenchmarkEntry benchmark : profile.benchmarks()) {
            byLabel.put(benchmark.label(), benchmark);
        }
        assertEquals(REQUIRED_NIGHTLY_STORAGE_CORE_LABELS, byLabel.keySet(),
                () -> "Unexpected benchmark labels in profile "
                        + profile.profile());

        assertEntry(byLabel.get("sorted-data-diff-key-read-compact"),
                "org.hestiastore.benchmark.sorteddatafile.DiffKeyReaderBenchmark",
                Map.of("entryCount", "1024", "keyLength", "24", "valueLength",
                        "16"));
        assertEntry(byLabel.get("sorted-data-diff-key-read-large"),
                "org.hestiastore.benchmark.sorteddatafile.DiffKeyReaderBenchmark",
                Map.of("entryCount", "8192", "keyLength", "48", "valueLength",
                        "64"));
        assertEntry(byLabel.get("single-chunk-entry-write-compact"),
                "org.hestiastore.benchmark.chunkentryfile.SingleChunkEntryWriterBenchmark",
                Map.of("entriesPerChunk", "64", "valueLength", "16"));
        assertEntry(byLabel.get("single-chunk-entry-write-large"),
                "org.hestiastore.benchmark.chunkentryfile.SingleChunkEntryWriterBenchmark",
                Map.of("entriesPerChunk", "1024", "valueLength", "64"));
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

    private String readUtf8(final Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to read " + path, e);
        }
    }

    record BenchmarkProfile(String profile, String description,
            List<BenchmarkEntry> benchmarks) {
    }

    record BenchmarkEntry(String label, String include, List<String> args) {
    }
}
