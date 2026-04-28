package org.hestiastore.indextools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.directory.FsNioDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexToolTest {

    @TempDir
    Path tempDir;

    @Test
    void exportBundleVerifyAndImportRoundTripsOfflineIndex() throws Exception {
        final Path sourceIndex = tempDir.resolve("source-index");
        final Path exportDirectory = tempDir.resolve("bundle-export");
        final Path importedIndex = tempDir.resolve("imported-index");
        createStringIndex(sourceIndex, List.of(Entry.of(1, "one"),
                Entry.of(2, "two"), Entry.of(3, "three")));

        assertSuccess(run("export", "--source-index", sourceIndex.toString(),
                "--output", exportDirectory.toString(), "--format", "bundle"));
        assertTrue(Files.exists(
                exportDirectory.resolve(ManifestSupport.MANIFEST_FILE_NAME)));
        assertTrue(Files.exists(
                exportDirectory.resolve(ManifestSupport.CONFIG_FILE_NAME)));

        assertSuccess(run("verify-export", "--input",
                exportDirectory.toString()));
        assertSuccess(run("inspect-export", "--input",
                exportDirectory.toString()));
        assertSuccess(run("import", "--input", exportDirectory.toString(),
                "--target-index", importedIndex.toString(),
                "--verify-after-import"));

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                new FsNioDirectory(importedIndex.toFile()))) {
            try (var stream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION)) {
                assertEquals(
                        List.of(Entry.of(1, "one"), Entry.of(2, "two"),
                                Entry.of(3, "three")),
                        stream.toList());
            }
        }
    }

    @Test
    void exportJsonlAndImportWithTargetConfigSupportsDatatypeMigration()
            throws Exception {
        final Path sourceIndex = tempDir.resolve("source-jsonl-index");
        final Path exportDirectory = tempDir.resolve("jsonl-export");
        final Path targetConfigPath = tempDir.resolve("target-config.json");
        final Path importedIndex = tempDir.resolve("jsonl-imported-index");
        createStringIndex(sourceIndex,
                List.of(Entry.of(1, "100"), Entry.of(2, "200")));

        assertSuccess(run("export", "--source-index", sourceIndex.toString(),
                "--output", exportDirectory.toString(), "--format", "jsonl"));
        assertSuccess(run("verify-export", "--input",
                exportDirectory.toString()));

        final IndexConfigurationManifest targetConfig = ManifestSupport
                .readJson(exportDirectory.resolve(ManifestSupport.CONFIG_FILE_NAME),
                        IndexConfigurationManifest.class);
        targetConfig.setIndexName("migrated-values");
        targetConfig.setValueClassName(Long.class.getName());
        targetConfig.setValueTypeDescriptor(
                "org.hestiastore.index.datatype.TypeDescriptorLong");
        ManifestSupport.writeJson(targetConfigPath, targetConfig);

        assertSuccess(run("import", "--input", exportDirectory.toString(),
                "--target-index", importedIndex.toString(), "--target-config",
                targetConfigPath.toString(), "--verify-after-import"));

        final IndexConfiguration<Integer, Long> configuration = IndexConfiguration
                .<Integer, Long>builder()
                .identity(identity -> identity.name("migrated-values")
                        .keyClass(Integer.class).valueClass(Long.class)
                        .keyTypeDescriptor(
                                "org.hestiastore.index.datatype.TypeDescriptorInteger")
                        .valueTypeDescriptor(
                                "org.hestiastore.index.datatype.TypeDescriptorLong"))
                .build();
        try (SegmentIndex<Integer, Long> index = SegmentIndex.open(
                new FsNioDirectory(importedIndex.toFile()), configuration)) {
            assertEquals(Long.valueOf(100L), index.get(1));
            assertEquals(Long.valueOf(200L), index.get(2));
            try (var stream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION)) {
                assertEquals(List.of(Entry.of(1, 100L), Entry.of(2, 200L)),
                        stream.toList());
            }
        }
    }

    @Test
    void commandHelpWorksWithoutRequiredParameters() {
        final CommandResult exportHelp = run("export", "-h");
        assertSuccess(exportHelp);
        assertTrue(exportHelp.stdout().contains("hestia_index export"));

        final CommandResult importHelp = run("import", "--help");
        assertSuccess(importHelp);
        assertTrue(importHelp.stdout().contains("hestia_index import"));

        final CommandResult delegatedHelp = run("help", "verify-export");
        assertSuccess(delegatedHelp);
        assertTrue(
                delegatedHelp.stdout().contains("hestia_index verify-export"));
    }

    @Test
    void groupedConfigurationRoundTripsThroughManifestMapper()
            throws Exception {
        final IndexConfiguration<Integer, String> configuration =
                IndexConfiguration.<Integer, String>builder()
                        .identity(identity -> identity.name("grouped-manifest")
                                .keyClass(Integer.class)
                                .valueClass(String.class)
                                .keyTypeDescriptor(
                                        "org.hestiastore.index.datatype.TypeDescriptorInteger")
                                .valueTypeDescriptor(
                                        "org.hestiastore.index.datatype.TypeDescriptorShortString"))
                        .segment(segment -> segment.maxKeys(100)
                                .chunkKeyLimit(10).cacheKeyLimit(20)
                                .cachedSegmentLimit(4)
                                .deltaCacheFileLimit(3))
                        .writePath(writePath -> writePath
                                .segmentWriteCacheKeyLimit(7)
                                .maintenanceWriteCacheKeyLimit(9)
                                .indexBufferedWriteKeyLimit(40)
                                .segmentSplitKeyThreshold(80))
                        .bloomFilter(bloom -> bloom.hashFunctions(2)
                                .indexSizeBytes(1024)
                                .falsePositiveProbability(0.05D))
                        .maintenance(maintenance -> maintenance
                                .segmentThreads(2).indexThreads(3)
                                .registryLifecycleThreads(4)
                                .busyBackoffMillis(5)
                                .busyTimeoutMillis(6)
                                .backgroundAutoEnabled(false))
                        .io(io -> io.diskBufferSizeBytes(2048))
                        .logging(logging -> logging.contextEnabled(false))
                        .filters(filters -> filters
                                .encodingFilterSpecs(
                                        List.of(ChunkFilterSpecs.doNothing()))
                                .decodingFilterSpecs(
                                        List.of(ChunkFilterSpecs.doNothing())))
                        .build();

        final IndexConfigurationManifest manifest = IndexConfigurationMapper
                .toManifest(configuration);
        final IndexConfiguration<?, ?> roundTrip = IndexConfigurationMapper
                .fromManifest(manifest);

        assertEquals("grouped-manifest", roundTrip.identity().name());
        assertEquals(Integer.valueOf(100), roundTrip.segment().maxKeys());
        assertEquals(Integer.valueOf(7),
                roundTrip.writePath().segmentWriteCacheKeyLimit());
        assertEquals(Integer.valueOf(1024),
                roundTrip.bloomFilter().indexSizeBytes());
        assertEquals(Integer.valueOf(2048),
                roundTrip.io().diskBufferSizeBytes());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                roundTrip.filters().encodingChunkFilterSpecs());
    }

    @Test
    void inspectExportCanReadMetadataWithoutFullVerification() throws Exception {
        final Path sourceIndex = tempDir.resolve("inspect-source-index");
        final Path exportDirectory = tempDir.resolve("inspect-export");
        createStringIndex(sourceIndex,
                List.of(Entry.of(1, "one"), Entry.of(2, "two")));

        assertSuccess(run("export", "--source-index", sourceIndex.toString(),
                "--output", exportDirectory.toString(), "--format", "bundle"));
        Files.delete(exportDirectory.resolve(ManifestSupport.CHECKSUMS_FILE_NAME));

        final CommandResult inspectResult = run("inspect-export", "--input",
                exportDirectory.toString());
        assertSuccess(inspectResult);
        assertTrue(
                inspectResult.stdout().contains("Verification: metadata-only"));

        final CommandResult verifiedInspect = run("inspect-export", "--input",
                exportDirectory.toString(), "--verify");
        assertEquals(1, verifiedInspect.exitCode());
        assertTrue(verifiedInspect.stderr().contains("checksums.txt"));

        final CommandResult verifyResult = run("verify-export", "--input",
                exportDirectory.toString());
        assertEquals(2, verifyResult.exitCode());
        assertTrue(verifyResult.stderr().contains("checksums.txt"));
    }

    @Test
    void exportSupportsRangeLimitAndJsonInspection() throws Exception {
        final Path sourceIndex = tempDir.resolve("range-source-index");
        final Path exportDirectory = tempDir.resolve("range-export");
        createStringKeyIndex(sourceIndex,
                List.of(Entry.of("a", "one"), Entry.of("b", "two"),
                        Entry.of("c", "three"), Entry.of("d", "four")));

        assertSuccess(run("export", "--source-index", sourceIndex.toString(),
                "--output", exportDirectory.toString(), "--format", "jsonl",
                "--compression", "none", "--from-key", "b", "--to-key", "d",
                "--limit", "2"));

        final CommandResult inspectJson = run("inspect-export", "--input",
                exportDirectory.toString(), "--json");
        assertSuccess(inspectJson);
        final Map<?, ?> inspect = ManifestSupport.mapper().readValue(
                inspectJson.stdout(), Map.class);
        assertEquals("metadata-only", inspect.get("verificationMode"));
        assertEquals("b", inspect.get("fromKey"));
        assertEquals("d", inspect.get("toKey"));
        assertEquals(2, ((Number) inspect.get("limit")).intValue());
        assertEquals(2, ((Number) inspect.get("recordCount")).intValue());

        final List<String> lines = Files
                .readAllLines(exportDirectory.resolve("data.jsonl"));
        assertEquals(2, lines.size());
    }

    @Test
    void initTargetConfigWritesTemplateWithOptionalIndexNameOverride()
            throws Exception {
        final Path sourceIndex = tempDir.resolve("template-source-index");
        final Path exportDirectory = tempDir.resolve("template-export");
        final Path templatePath = tempDir.resolve("generated-target-config.json");
        createStringIndex(sourceIndex, List.of(Entry.of(1, "one")));

        assertSuccess(run("export", "--source-index", sourceIndex.toString(),
                "--output", exportDirectory.toString(), "--format", "jsonl"));
        assertSuccess(run("init-target-config", "--input",
                exportDirectory.toString(), "--output",
                templatePath.toString(), "--index-name", "renamed-target"));

        final IndexConfigurationManifest template = ManifestSupport
                .readJson(templatePath, IndexConfigurationManifest.class);
        assertEquals("renamed-target", template.getIndexName());
        assertEquals(String.class.getName(), template.getValueClassName());
    }

    @Test
    void verifyAndInspectSupportJsonOutput() throws Exception {
        final Path sourceIndex = tempDir.resolve("json-source-index");
        final Path exportDirectory = tempDir.resolve("json-export");
        createStringIndex(sourceIndex, List.of(Entry.of(1, "one")));

        assertSuccess(run("export", "--source-index", sourceIndex.toString(),
                "--output", exportDirectory.toString(), "--format", "bundle"));

        final CommandResult inspectJson = run("inspect-export", "--input",
                exportDirectory.toString(), "--json", "--verify");
        assertSuccess(inspectJson);
        final Map<?, ?> inspect = ManifestSupport.mapper().readValue(
                inspectJson.stdout(), Map.class);
        assertEquals("full", inspect.get("verificationMode"));

        final CommandResult verifyJson = run("verify-export", "--input",
                exportDirectory.toString(), "--json");
        assertSuccess(verifyJson);
        final Map<?, ?> verify = ManifestSupport.mapper().readValue(
                verifyJson.stdout(), Map.class);
        assertEquals("full", verify.get("verificationMode"));
        assertEquals("bundle", verify.get("format"));
    }

    @Test
    void compatibilityBundleFixtureVerifiesAndImports() throws Exception {
        final Path fixture = fixturePath("/compatibility/v1/bundle-export");
        final Path importedIndex = tempDir.resolve("bundle-fixture-imported");

        assertSuccess(run("verify-export", "--input", fixture.toString()));
        assertSuccess(run("import", "--input", fixture.toString(),
                "--target-index", importedIndex.toString(),
                "--verify-after-import"));

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                new FsNioDirectory(importedIndex.toFile()))) {
            assertEquals("one", index.get(1));
            assertEquals("two", index.get(2));
        }
    }

    @Test
    void compatibilityJsonlFixtureVerifiesAndImports() throws Exception {
        final Path fixture = fixturePath("/compatibility/v1/jsonl-export");
        final Path importedIndex = tempDir.resolve("jsonl-fixture-imported");

        assertSuccess(run("verify-export", "--input", fixture.toString()));
        assertSuccess(run("import", "--input", fixture.toString(),
                "--target-index", importedIndex.toString(),
                "--verify-after-import"));

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                new FsNioDirectory(importedIndex.toFile()))) {
            assertEquals("one", index.get(1));
            assertEquals("two", index.get(2));
        }
    }

    private void createStringIndex(final Path indexPath,
            final List<Entry<Integer, String>> entries) {
        final IndexConfiguration<Integer, String> configuration = IndexConfiguration
                .<Integer, String>builder()
                .identity(identity -> identity.name("source")
                        .keyClass(Integer.class).valueClass(String.class))
                .build();
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                new FsNioDirectory(indexPath.toFile()), configuration)) {
            entries.forEach(index::put);
            index.flushAndWait();
            index.compactAndWait();
        }
    }

    private void createStringKeyIndex(final Path indexPath,
            final List<Entry<String, String>> entries) {
        final IndexConfiguration<String, String> configuration = IndexConfiguration
                .<String, String>builder()
                .identity(identity -> identity.name("string-keys")
                        .keyClass(String.class).valueClass(String.class))
                .build();
        try (SegmentIndex<String, String> index = SegmentIndex.create(
                new FsNioDirectory(indexPath.toFile()), configuration)) {
            entries.forEach(index::put);
            index.flushAndWait();
            index.compactAndWait();
        }
    }

    private Path fixturePath(final String resourcePath)
            throws URISyntaxException {
        final java.net.URL resource = IndexToolTest.class
                .getResource(resourcePath);
        if (resource == null) {
            fail("Missing fixture resource: " + resourcePath);
        }
        return Path.of(resource.toURI());
    }

    private CommandResult run(final String... args) {
        final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
        final int exitCode = IndexTool.run(args, new PrintStream(outBuffer),
                new PrintStream(errBuffer));
        return new CommandResult(exitCode, outBuffer.toString(),
                errBuffer.toString());
    }

    private void assertSuccess(final CommandResult result) {
        if (result.exitCode() != 0) {
            fail("CLI failed with exit code " + result.exitCode() + "\nSTDOUT:\n"
                    + result.stdout() + "\nSTDERR:\n" + result.stderr());
        }
    }

    private static final class CommandResult {

        private final int exitCode;
        private final String stdout;
        private final String stderr;

        private CommandResult(final int exitCode, final String stdout,
                final String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        int exitCode() {
            return exitCode;
        }

        String stdout() {
            return stdout;
        }

        String stderr() {
            return stderr;
        }
    }
}
