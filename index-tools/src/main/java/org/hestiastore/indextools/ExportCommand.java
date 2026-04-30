package org.hestiastore.indextools;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.FsNioDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;

final class ExportCommand {

    private static final long DEFAULT_MAX_PART_SIZE_BYTES = 512L * 1024L
            * 1024L;

    int run(final String[] args, final PrintStream out, final PrintStream err) {
        final Options options = new Options();
        options.addOption(Option.builder().longOpt("source-index").hasArg()
                .desc("Path to an offline source index directory.").required()
                .get());
        options.addOption(Option.builder().longOpt("output").hasArg()
                .desc("Output directory for the export bundle.").required()
                .get());
        options.addOption(Option.builder().longOpt("format").hasArg()
                .desc("Export format: bundle or jsonl. Default: bundle.")
                .get());
        options.addOption(Option.builder().longOpt("compression").hasArg()
                .desc("Compression mode: gzip or none. Default: gzip.")
                .get());
        options.addOption(Option.builder().longOpt("max-part-size").hasArg()
                .desc("Approximate uncompressed bundle part size. Default: 512MiB.")
                .get());
        options.addOption(Option.builder().longOpt("from-key").hasArg()
                .desc("Inclusive lower key bound for a partial export.")
                .get());
        options.addOption(Option.builder().longOpt("to-key").hasArg()
                .desc("Inclusive upper key bound for a partial export.")
                .get());
        options.addOption(Option.builder().longOpt("limit").hasArg()
                .desc("Maximum number of exported records after range filtering.")
                .get());
        options.addOption(Option.builder().longOpt("overwrite")
                .desc("Delete an existing output directory first.").get());
        options.addOption(
                Option.builder("h").longOpt("help").desc("Show help.").get());
        try {
            if (HelpSupport.isHelpRequested(args)) {
                printHelp(options, out);
                return 0;
            }
            final CommandLine commandLine = new DefaultParser()
                    .parse(options, args);
            final Path sourceIndex = Path
                    .of(commandLine.getOptionValue("source-index"))
                    .toAbsolutePath().normalize();
            final Path outputDirectory = Path.of(commandLine.getOptionValue("output"))
                    .toAbsolutePath().normalize();
            final ExportFormat format = commandLine.hasOption("format")
                    ? ExportFormat.parse(commandLine.getOptionValue("format"))
                    : ExportFormat.BUNDLE;
            final CompressionMode compression = commandLine
                    .hasOption("compression")
                            ? CompressionMode.parse(commandLine
                                    .getOptionValue("compression"))
                            : CompressionMode.GZIP;
            final long maxPartSizeBytes = commandLine
                    .hasOption("max-part-size")
                            ? SizeValueParser.parseBytes(commandLine
                                    .getOptionValue("max-part-size"))
                            : DEFAULT_MAX_PART_SIZE_BYTES;
            final String fromKey = commandLine.getOptionValue("from-key");
            final String toKey = commandLine.getOptionValue("to-key");
            final Long limit = commandLine.hasOption("limit")
                    ? Long.valueOf(commandLine.getOptionValue("limit"))
                    : null;
            final boolean overwrite = commandLine.hasOption("overwrite");
            execute(sourceIndex, outputDirectory, format, compression,
                    maxPartSizeBytes, fromKey, toKey, limit, overwrite, out);
            return 0;
        } catch (final Exception e) {
            err.println("Export failed: " + e.getMessage());
            if (!(e instanceof org.apache.commons.cli.ParseException)) {
                e.printStackTrace(err);
            } else {
                printHelp(options, err);
            }
            return 1;
        }
    }

    private void execute(final Path sourceIndex, final Path outputDirectory,
            final ExportFormat format, final CompressionMode compression,
            final long maxPartSizeBytes, final String fromKey,
            final String toKey, final Long limit, final boolean overwrite,
            final PrintStream out) throws Exception {
        if (!Files.isDirectory(sourceIndex)) {
            throw new IOException(
                    "Source index directory does not exist: " + sourceIndex);
        }
        PathSupport.prepareOutputDirectory(outputDirectory, overwrite);
        final ChunkFilterProviderResolver resolver = ClasspathExtensionSupport
                .chunkFilterProviderResolver();
        final ExportBundleManifest manifest = new ExportBundleManifest();
        manifest.setFormatVersion(1);
        manifest.setToolVersion(ClasspathExtensionSupport.toolVersion());
        manifest.setCreatedAt(Instant.now());
        manifest.setSourceIndexPath(sourceIndex.toString());
        manifest.setFormat(format);
        manifest.setCompression(compression);
        manifest.setMaxPartSizeBytes(maxPartSizeBytes);
        manifest.setConfigFileName(ManifestSupport.CONFIG_FILE_NAME);

        try (SegmentIndex<Object, Object> index = openIndex(sourceIndex,
                resolver)) {
            final IndexConfiguration<?, ?> configuration = index
                    .getConfiguration();
            final IndexConfigurationManifest sourceConfiguration = IndexConfigurationMapper
                    .toManifest(configuration);
            manifest.setSourceConfiguration(sourceConfiguration);
            final DescriptorSupport.DescriptorPair descriptors = DescriptorSupport
                    .fromConfiguration(configuration);
            final ExportSelection selection = ExportSelection.create(fromKey,
                    toKey, limit, descriptors.key());
            manifest.setFromKeyText(selection.getFromKeyText());
            manifest.setToKeyText(selection.getToKeyText());
            manifest.setLimit(selection.getLimit());
            final long recordCount = format == ExportFormat.BUNDLE
                    ? exportBundle(outputDirectory, manifest, index, descriptors,
                            selection)
                    : exportJsonl(outputDirectory, manifest, index, descriptors,
                            selection);
            manifest.setRecordCount(recordCount);
            ManifestSupport.writeJson(
                    outputDirectory.resolve(ManifestSupport.CONFIG_FILE_NAME),
                    sourceConfiguration);
            ManifestSupport.writeJson(
                    outputDirectory.resolve(ManifestSupport.MANIFEST_FILE_NAME),
                    manifest);
            writeChecksums(outputDirectory, manifest);
            out.printf(
                    "Exported %d records from %s into %s (%s, %s).%n",
                    recordCount, sourceIndex, outputDirectory,
                    manifest.getFormat().name().toLowerCase(),
                    manifest.getCompression().name().toLowerCase());
            if (selection.isPartial()) {
                out.printf("Selection: from=%s to=%s limit=%s%n",
                        selection.getFromKeyText(), selection.getToKeyText(),
                        selection.getLimit());
            }
        }
    }

    private long exportBundle(final Path outputDirectory,
            final ExportBundleManifest manifest,
            final SegmentIndex<Object, Object> index,
            final DescriptorSupport.DescriptorPair descriptors,
            final ExportSelection selection)
            throws IOException {
        final AtomicLong recordCount = new AtomicLong();
        final BundlePartWriter writer = new BundlePartWriter(outputDirectory,
                manifest.getCompression(), manifest.getMaxPartSizeBytes());
        try (Stream<Entry<Object, Object>> stream = index
                .getStream(SegmentWindow.unbounded(),
                        SegmentIteratorIsolation.FULL_ISOLATION)) {
            selection.apply(stream).forEach(entry -> {
                try {
                    final byte[] keyBytes = DescriptorSupport
                            .encode(descriptors.key().getDescriptor(),
                                    entry.getKey());
                    final byte[] valueBytes = DescriptorSupport
                            .encode(descriptors.value().getDescriptor(),
                                    entry.getValue());
                    writer.writeRecord(keyBytes, valueBytes);
                    recordCount.incrementAndGet();
                } catch (final IOException e) {
                    throw new IllegalStateException("Unable to export entry.",
                            e);
                }
            });
        } catch (final IllegalStateException e) {
            if (e.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw e;
        } finally {
            writer.close();
        }
        manifest.setParts(writer.getPartManifests());
        return recordCount.get();
    }

    private long exportJsonl(final Path outputDirectory,
            final ExportBundleManifest manifest,
            final SegmentIndex<Object, Object> index,
            final DescriptorSupport.DescriptorPair descriptors,
            final ExportSelection selection)
            throws IOException {
        final TextValueCodecRegistry textCodecs = new TextValueCodecRegistry();
        final String dataFileName = manifest.getCompression() == CompressionMode.GZIP
                ? "data.jsonl.gz"
                : "data.jsonl";
        manifest.setDataFileName(dataFileName);
        final Path dataFile = outputDirectory.resolve(dataFileName);
        long recordCount = 0L;
        try (OutputStream output = newBufferedFileOutput(dataFile,
                manifest.getCompression())) {
            try (BufferedWriter writer = new java.io.BufferedWriter(
                    new java.io.OutputStreamWriter(output,
                            StandardCharsets.UTF_8))) {
                try (Stream<Entry<Object, Object>> stream = index
                        .getStream(SegmentWindow.unbounded(),
                                SegmentIteratorIsolation.FULL_ISOLATION)) {
                    for (final java.util.Iterator<Entry<Object, Object>> iterator = selection
                            .apply(stream).iterator(); iterator.hasNext();) {
                        final Entry<Object, Object> entry = iterator.next();
                        final JsonLineRecord record = new JsonLineRecord();
                        record.setKey(DescriptorSupport.toEnvelope(
                                descriptors.key(), entry.getKey(), textCodecs));
                        record.setValue(DescriptorSupport.toEnvelope(
                                descriptors.value(), entry.getValue(),
                                textCodecs));
                        writer.write(ManifestSupport.lineMapper()
                                .writeValueAsString(record));
                        writer.newLine();
                        recordCount++;
                    }
                }
            }
        }
        return recordCount;
    }

    private OutputStream newBufferedFileOutput(final Path path,
            final CompressionMode compression) throws IOException {
        final OutputStream raw = Files.newOutputStream(path);
        if (compression == CompressionMode.GZIP) {
            return new GZIPOutputStream(raw);
        }
        return raw;
    }

    private void writeChecksums(final Path outputDirectory,
            final ExportBundleManifest manifest) throws IOException {
        final LinkedHashMap<String, String> checksums = new LinkedHashMap<>();
        addChecksum(checksums, outputDirectory,
                ManifestSupport.MANIFEST_FILE_NAME);
        addChecksum(checksums, outputDirectory, manifest.getConfigFileName());
        if (manifest.getFormat() == ExportFormat.BUNDLE) {
            for (final ExportPartManifest part : manifest.getParts()) {
                addChecksum(checksums, outputDirectory, part.getFileName());
            }
        } else {
            addChecksum(checksums, outputDirectory, manifest.getDataFileName());
        }
        ChecksumSupport.writeChecksums(
                outputDirectory.resolve(ManifestSupport.CHECKSUMS_FILE_NAME),
                checksums);
    }

    private void addChecksum(final Map<String, String> checksums,
            final Path outputDirectory, final String fileName)
            throws IOException {
        checksums.put(fileName,
                ChecksumSupport.sha256(outputDirectory.resolve(fileName)));
    }

    private SegmentIndex<Object, Object> openIndex(final Path sourceIndex,
            final ChunkFilterProviderResolver resolver) {
        return SegmentIndex.open(new FsNioDirectory(sourceIndex.toFile()),
                resolver);
    }

    private void printHelp(final Options options, final PrintStream out) {
        HelpSupport.printHelp(out, "hestia_index export", options);
    }

    private static final class BundlePartWriter implements AutoCloseable {

        private final Path outputDirectory;
        private final CompressionMode compression;
        private final long maxPartSizeBytes;
        private final java.util.List<ExportPartManifest> partManifests = new java.util.ArrayList<>();
        private int partIndex;
        private CountingOutputStream countingOutput;
        private DataOutputStream currentOutput;
        private Path currentFile;
        private long currentRecords;

        BundlePartWriter(final Path outputDirectory,
                final CompressionMode compression, final long maxPartSizeBytes) {
            this.outputDirectory = outputDirectory;
            this.compression = compression;
            this.maxPartSizeBytes = maxPartSizeBytes;
        }

        void writeRecord(final byte[] keyBytes, final byte[] valueBytes)
                throws IOException {
            if (currentOutput == null) {
                openNextPart();
            }
            final long recordSize = (long) Integer.BYTES + Integer.BYTES
                    + keyBytes.length + valueBytes.length;
            if (currentRecords > 0L
                    && countingOutput.getWrittenBytes() + recordSize > maxPartSizeBytes) {
                closeCurrentPart();
                openNextPart();
            }
            currentOutput.writeInt(keyBytes.length);
            currentOutput.writeInt(valueBytes.length);
            currentOutput.write(keyBytes);
            currentOutput.write(valueBytes);
            currentRecords++;
        }

        java.util.List<ExportPartManifest> getPartManifests() {
            return partManifests;
        }

        @Override
        public void close() throws IOException {
            closeCurrentPart();
        }

        private void openNextPart() throws IOException {
            partIndex++;
            currentRecords = 0L;
            final String fileName = String.format("part-%06d.bin%s", partIndex,
                    compression == CompressionMode.GZIP ? ".gz" : "");
            currentFile = outputDirectory.resolve(fileName);
            OutputStream raw = Files.newOutputStream(currentFile);
            if (compression == CompressionMode.GZIP) {
                raw = new GZIPOutputStream(raw);
            }
            countingOutput = new CountingOutputStream(raw);
            currentOutput = new DataOutputStream(
                    new BufferedOutputStream(countingOutput));
        }

        private void closeCurrentPart() throws IOException {
            if (currentOutput == null) {
                return;
            }
            currentOutput.close();
            final ExportPartManifest part = new ExportPartManifest();
            part.setFileName(currentFile.getFileName().toString());
            part.setRecordCount(currentRecords);
            part.setEncodedBytes(countingOutput.getWrittenBytes());
            part.setFileSizeBytes(Files.size(currentFile));
            partManifests.add(part);
            currentOutput = null;
            countingOutput = null;
            currentFile = null;
        }
    }
}
