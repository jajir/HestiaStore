package org.hestiastore.indextools;

import java.io.PrintStream;
import java.nio.file.Path;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

final class InspectExportCommand {

    int run(final String[] args, final PrintStream out, final PrintStream err) {
        final Options options = new Options();
        options.addOption(Option.builder().longOpt("input").hasArg()
                .desc("Export directory to inspect.").required().build());
        options.addOption(Option.builder().longOpt("verify")
                .desc("Also verify checksums and record counts.").build());
        options.addOption(Option.builder().longOpt("json")
                .desc("Print inspection result as JSON.").build());
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
            final boolean verify = commandLine.hasOption("verify");
            final boolean json = commandLine.hasOption("json");
            final ExportBundleManifest manifest = verify
                    ? VerifyExportCommand.verify(inputDirectory)
                    : VerifyExportCommand.inspect(inputDirectory);
            if (json) {
                CommandJsonSupport.printJson(out,
                        ManifestReportSupport.summary(inputDirectory, manifest,
                                verify ? "full" : "metadata-only"));
            } else {
                out.printf("Export directory: %s%n", inputDirectory);
                out.printf("Format: %s%n",
                        manifest.getFormat().name().toLowerCase());
                out.printf("Compression: %s%n",
                        manifest.getCompression().name().toLowerCase());
                out.printf("Created at: %s%n", manifest.getCreatedAt());
                out.printf("Source index: %s%n",
                        manifest.getSourceConfiguration().getIndexName());
                out.printf("Source path: %s%n", manifest.getSourceIndexPath());
                out.printf("Record count: %d%n", manifest.getRecordCount());
                out.printf("Config file: %s%n", manifest.getConfigFileName());
                if (manifest.getFromKeyText() != null
                        || manifest.getToKeyText() != null
                        || manifest.getLimit() != null) {
                    out.printf("Selection: from=%s to=%s limit=%s%n",
                            manifest.getFromKeyText(), manifest.getToKeyText(),
                            manifest.getLimit());
                }
                if (manifest.getFormat() == ExportFormat.BUNDLE) {
                    out.printf("Parts: %d%n", manifest.getParts().size());
                    for (final ExportPartManifest part : manifest.getParts()) {
                        out.printf("  %s records=%d size=%d bytes%n",
                                part.getFileName(), part.getRecordCount(),
                                part.getFileSizeBytes());
                    }
                } else {
                    out.printf("Data file: %s%n", manifest.getDataFileName());
                }
                out.printf("Verification: %s%n",
                        verify ? "full" : "metadata-only");
            }
            return 0;
        } catch (final Exception e) {
            err.println("Export inspection failed: " + e.getMessage());
            if (!(e instanceof org.apache.commons.cli.ParseException)) {
                e.printStackTrace(err);
            } else {
                printHelp(options, err);
            }
            return 1;
        }
    }

    private void printHelp(final Options options, final PrintStream out) {
        HelpSupport.printHelp(out, "hestia_index inspect-export", options);
    }
}
