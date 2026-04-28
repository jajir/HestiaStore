package org.hestiastore.indextools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.directory.FsNioDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;

final class ImportCommand {

    private static final long FLUSH_INTERVAL = 10_000L;

    int run(final String[] args, final PrintStream out, final PrintStream err) {
        final Options options = new Options();
        options.addOption(Option.builder().longOpt("input").hasArg()
                .desc("Export directory to import from.").required().get());
        options.addOption(Option.builder().longOpt("target-index").hasArg()
                .desc("Target index directory to create.").required().get());
        options.addOption(Option.builder().longOpt("target-config").hasArg()
                .desc("Optional JSON configuration manifest overriding the exported source-config.json.")
                .get());
        options.addOption(Option.builder().longOpt("index-name").hasArg()
                .desc("Optional index name override applied after loading target config.")
                .get());
        options.addOption(Option.builder().longOpt("verify-after-import")
                .desc("Reopen the target index and compare record count and logical fingerprint.")
                .get());
        options.addOption(
                Option.builder("h").longOpt("help").desc("Show help.").get());
        try {
            if (HelpSupport.isHelpRequested(args)) {
                printHelp(options, out);
                return 0;
            }
            final CommandLine commandLine = new DefaultParser()
                    .parse(options, args);
            final Path inputDirectory = Path.of(commandLine.getOptionValue("input"))
                    .toAbsolutePath().normalize();
            final Path targetIndex = Path
                    .of(commandLine.getOptionValue("target-index"))
                    .toAbsolutePath().normalize();
            final Path targetConfigPath = commandLine.hasOption("target-config")
                    ? Path.of(commandLine.getOptionValue("target-config"))
                            .toAbsolutePath().normalize()
                    : null;
            final String indexName = commandLine.getOptionValue("index-name");
            final boolean verifyAfterImport = commandLine
                    .hasOption("verify-after-import");
            execute(inputDirectory, targetIndex, targetConfigPath, indexName,
                    verifyAfterImport, out);
            return 0;
        } catch (final Exception e) {
            err.println("Import failed: " + e.getMessage());
            if (!(e instanceof org.apache.commons.cli.ParseException)) {
                e.printStackTrace(err);
            } else {
                printHelp(options, err);
            }
            return 1;
        }
    }

    private void execute(final Path inputDirectory, final Path targetIndex,
            final Path targetConfigPath, final String indexName,
            final boolean verifyAfterImport,
            final PrintStream out) throws Exception {
        final ExportBundleManifest manifest = VerifyExportCommand
                .verify(inputDirectory);
        PathSupport.ensureEmptyDirectory(targetIndex);
        IndexConfigurationManifest configurationManifest = targetConfigPath == null
                ? ManifestSupport.readJson(
                        inputDirectory.resolve(manifest.getConfigFileName()),
                        IndexConfigurationManifest.class)
                : ManifestSupport.readJson(targetConfigPath,
                        IndexConfigurationManifest.class);
        if (indexName != null && !indexName.isBlank()) {
            configurationManifest = IndexConfigurationMapper
                    .withIndexName(configurationManifest, indexName);
        }
        final IndexConfiguration<?, ?> configuration = IndexConfigurationMapper
                .fromManifest(configurationManifest);
        final DescriptorSupport.DescriptorPair descriptors = DescriptorSupport
                .fromConfiguration(configuration);
        final ChunkFilterProviderRegistry registry = ClasspathExtensionSupport
                .chunkFilterProviderRegistry();
        final LogicalFingerprint sourceFingerprint = verifyAfterImport
                ? new LogicalFingerprint()
                : null;
        final long importedRecords;
        try (SegmentIndex<Object, Object> index = createIndex(targetIndex,
                configuration, registry)) {
            importedRecords = manifest.getFormat() == ExportFormat.BUNDLE
                    ? importBundle(inputDirectory, manifest, descriptors, index,
                            sourceFingerprint)
                    : importJsonl(inputDirectory, manifest, descriptors, index,
                            sourceFingerprint);
            index.flushAndWait();
            index.compactAndWait();
            index.checkAndRepairConsistency();
        }
        out.printf("Imported %d records into %s.%n", importedRecords,
                targetIndex);
        if (verifyAfterImport) {
            final ImportVerificationSummary verificationSummary = verifyImport(
                    targetIndex, configuration, descriptors, importedRecords,
                    sourceFingerprint);
            out.printf(
                    "Verified imported index (%d records, fingerprint %s).%n",
                    verificationSummary.getRecordCount(),
                    verificationSummary.getTargetFingerprint());
        }
    }

    private long importBundle(final Path inputDirectory,
            final ExportBundleManifest manifest,
            final DescriptorSupport.DescriptorPair descriptors,
            final SegmentIndex<Object, Object> index,
            final LogicalFingerprint fingerprint) throws IOException {
        long imported = 0L;
        for (final ExportPartManifest part : manifest.getParts()) {
            try (InputStream input = VerifyExportCommand.newInputStream(
                    inputDirectory.resolve(part.getFileName()),
                    manifest.getCompression())) {
                try (DataInputStream dataInput = new DataInputStream(
                        new BufferedInputStream(input))) {
                    while (true) {
                        try {
                            final int keyLength = dataInput.readInt();
                            final int valueLength = dataInput.readInt();
                            final byte[] keyBytes = dataInput
                                    .readNBytes(keyLength);
                            final byte[] valueBytes = dataInput
                                    .readNBytes(valueLength);
                            final Object key = descriptors.key().getDescriptor()
                                    .getTypeDecoder().decode(keyBytes);
                            final Object value = descriptors.value()
                                    .getDescriptor().getTypeDecoder()
                                    .decode(valueBytes);
                            index.put(key, value);
                            updateFingerprint(fingerprint, descriptors, key,
                                    value);
                            imported++;
                            flushIfNeeded(imported, index);
                        } catch (final java.io.EOFException eofException) {
                            break;
                        }
                    }
                }
            }
        }
        return imported;
    }

    private long importJsonl(final Path inputDirectory,
            final ExportBundleManifest manifest,
            final DescriptorSupport.DescriptorPair descriptors,
            final SegmentIndex<Object, Object> index,
            final LogicalFingerprint fingerprint) throws IOException {
        final TextValueCodecRegistry textCodecs = new TextValueCodecRegistry();
        long imported = 0L;
        try (InputStream input = VerifyExportCommand.newInputStream(
                inputDirectory.resolve(manifest.getDataFileName()),
                manifest.getCompression())) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(input,
                            java.nio.charset.StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                while (line != null) {
                    if (line.isBlank()) {
                        line = reader.readLine();
                        continue;
                    }
                    final JsonLineRecord record = ManifestSupport.lineMapper()
                            .readValue(line, JsonLineRecord.class);
                    final Object key = DescriptorSupport.fromEnvelope(
                            descriptors.key(), record.getKey(), textCodecs);
                    final Object value = DescriptorSupport.fromEnvelope(
                            descriptors.value(), record.getValue(), textCodecs);
                    index.put(key, value);
                    updateFingerprint(fingerprint, descriptors, key, value);
                    imported++;
                    flushIfNeeded(imported, index);
                    line = reader.readLine();
                }
            }
        }
        return imported;
    }

    private void flushIfNeeded(final long imported,
            final SegmentIndex<Object, Object> index) {
        if (imported % FLUSH_INTERVAL == 0L) {
            index.flushAndWait();
        }
    }

    private ImportVerificationSummary verifyImport(final Path targetIndex,
            final IndexConfiguration<?, ?> configuration,
            final DescriptorSupport.DescriptorPair descriptors,
            final long expectedRecordCount, final LogicalFingerprint source)
            throws Exception {
        final LogicalFingerprint targetFingerprint = new LogicalFingerprint();
        try (SegmentIndex<Object, Object> index = openIndex(targetIndex,
                configuration)) {
            try (Stream<Entry<Object, Object>> stream = index
                    .getStream(SegmentWindow.unbounded(),
                            SegmentIteratorIsolation.FULL_ISOLATION)) {
                stream.forEach(entry -> targetFingerprint.update(descriptors,
                        entry.getKey(), entry.getValue()));
            }
        }
        if (targetFingerprint.getRecordCount() != expectedRecordCount) {
            throw new IOException(String.format(
                    "Post-import verification count mismatch. Expected=%s actual=%s",
                    expectedRecordCount, targetFingerprint.getRecordCount()));
        }
        final String sourceFingerprint = source.hexDigest();
        final String targetDigest = targetFingerprint.hexDigest();
        if (!sourceFingerprint.equals(targetDigest)) {
            throw new IOException(
                    "Post-import verification fingerprint mismatch.");
        }
        return new ImportVerificationSummary(targetFingerprint.getRecordCount(),
                sourceFingerprint, targetDigest);
    }

    private void updateFingerprint(final LogicalFingerprint fingerprint,
            final DescriptorSupport.DescriptorPair descriptors, final Object key,
            final Object value) {
        if (fingerprint != null) {
            fingerprint.update(descriptors, key, value);
        }
    }

    @SuppressWarnings("unchecked")
    private SegmentIndex<Object, Object> createIndex(final Path targetIndex,
            final IndexConfiguration<?, ?> configuration,
            final ChunkFilterProviderRegistry registry) {
        return (SegmentIndex<Object, Object>) SegmentIndex.create(
                new FsNioDirectory(targetIndex.toFile()),
                (IndexConfiguration<Object, Object>) configuration, registry);
    }

    @SuppressWarnings("unchecked")
    private SegmentIndex<Object, Object> openIndex(final Path targetIndex,
            final IndexConfiguration<?, ?> configuration) {
        return (SegmentIndex<Object, Object>) SegmentIndex.open(
                new FsNioDirectory(targetIndex.toFile()),
                (IndexConfiguration<Object, Object>) configuration);
    }

    private void printHelp(final Options options, final PrintStream out) {
        HelpSupport.printHelp(out, "hestia_index import", options);
    }
}
