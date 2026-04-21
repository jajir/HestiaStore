package org.hestiastore.indextools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.fasterxml.jackson.databind.ObjectMapper;

final class VerifyExportCommand {

    int run(final String[] args, final PrintStream out, final PrintStream err) {
        final Options options = new Options();
        options.addOption(Option.builder().longOpt("input").hasArg()
                .desc("Export directory to verify.").required().build());
        options.addOption(Option.builder().longOpt("json")
                .desc("Print verification result as JSON.").build());
        options.addOption(
                Option.builder("h").longOpt("help").desc("Show help.").build());
        try {
            if (HelpSupport.isHelpRequested(args)) {
                printHelp(options, out);
                return 0;
            }
            final CommandLine commandLine = new DefaultParser()
                    .parse(options, args);
            final Path inputDirectory = Path.of(commandLine.getOptionValue("input"))
                    .toAbsolutePath().normalize();
            final boolean json = commandLine.hasOption("json");
            final ExportBundleManifest manifest = verify(inputDirectory);
            if (json) {
                CommandJsonSupport.printJson(out,
                        ManifestReportSupport.summary(inputDirectory, manifest,
                                "full"));
            } else {
                out.printf("Verified export %s (%s, %d records).%n",
                        inputDirectory,
                        manifest.getFormat().name().toLowerCase(),
                        manifest.getRecordCount());
            }
            return 0;
        } catch (final Exception e) {
            err.println("Export verification failed: " + e.getMessage());
            if (!(e instanceof org.apache.commons.cli.ParseException)) {
                e.printStackTrace(err);
            } else {
                printHelp(options, err);
            }
            return 2;
        }
    }

    static ExportBundleManifest verify(final Path inputDirectory)
            throws IOException {
        final ExportBundleManifest manifest = inspect(inputDirectory);
        verifyChecksums(inputDirectory);
        final long countedRecords = manifest.getFormat() == ExportFormat.BUNDLE
                ? countBundleRecords(inputDirectory, manifest)
                : countJsonlRecords(inputDirectory, manifest);
        if (countedRecords != manifest.getRecordCount()) {
            throw new IOException(String.format(
                    "Record count mismatch. Manifest=%s actual=%s",
                    manifest.getRecordCount(), countedRecords));
        }
        return manifest;
    }

    static ExportBundleManifest inspect(final Path inputDirectory)
            throws IOException {
        if (!Files.isDirectory(inputDirectory)) {
            throw new IOException(
                    "Export directory does not exist: " + inputDirectory);
        }
        final ExportBundleManifest manifest = ManifestSupport.readJson(
                inputDirectory.resolve(ManifestSupport.MANIFEST_FILE_NAME),
                ExportBundleManifest.class);
        final IndexConfigurationManifest configuration = ManifestSupport
                .readJson(inputDirectory.resolve(manifest.getConfigFileName()),
                        IndexConfigurationManifest.class);
        if (!manifest.getSourceConfiguration().getIndexName()
                .equals(configuration.getIndexName())) {
            throw new IOException(
                    "Manifest and source-config.json describe different indexes.");
        }
        return manifest;
    }

    private static void verifyChecksums(final Path inputDirectory)
            throws IOException {
        final Map<String, String> checksums = ChecksumSupport
                .readChecksums(inputDirectory
                        .resolve(ManifestSupport.CHECKSUMS_FILE_NAME));
        for (final var entry : checksums.entrySet()) {
            final Path file = inputDirectory.resolve(entry.getKey());
            if (!Files.exists(file)) {
                throw new IOException("Missing exported file: " + file);
            }
            final String actual = ChecksumSupport.sha256(file);
            if (!entry.getValue().equals(actual)) {
                throw new IOException(
                        "Checksum mismatch for " + file.getFileName());
            }
        }
    }

    private static long countBundleRecords(final Path inputDirectory,
            final ExportBundleManifest manifest) throws IOException {
        long count = 0L;
        for (final ExportPartManifest part : manifest.getParts()) {
            try (InputStream input = newInputStream(
                    inputDirectory.resolve(part.getFileName()),
                    manifest.getCompression())) {
                try (DataInputStream dataInput = new DataInputStream(
                new BufferedInputStream(input))) {
                    while (true) {
                        try {
                            final int keyLength = dataInput.readInt();
                            final int valueLength = dataInput.readInt();
                            dataInput.skipNBytes(
                                    (long) keyLength + valueLength);
                            count++;
                        } catch (final java.io.EOFException eofException) {
                            break;
                        }
                    }
                }
            }
        }
        return count;
    }

    private static long countJsonlRecords(final Path inputDirectory,
            final ExportBundleManifest manifest) throws IOException {
        final ObjectMapper mapper = ManifestSupport.mapper();
        long count = 0L;
        try (InputStream input = newInputStream(
                inputDirectory.resolve(manifest.getDataFileName()),
                manifest.getCompression())) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(input, java.nio.charset.StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                while (line != null) {
                    if (line.isBlank()) {
                        line = reader.readLine();
                        continue;
                    }
                    mapper.readValue(line, JsonLineRecord.class);
                    count++;
                    line = reader.readLine();
                }
            }
        }
        return count;
    }

    static InputStream newInputStream(final Path path,
            final CompressionMode compression) throws IOException {
        final InputStream input = Files.newInputStream(path);
        if (compression == CompressionMode.GZIP) {
            return new GZIPInputStream(input);
        }
        return input;
    }

    private void printHelp(final Options options, final PrintStream out) {
        HelpSupport.printHelp(out, "hestia_index verify-export", options);
    }
}
