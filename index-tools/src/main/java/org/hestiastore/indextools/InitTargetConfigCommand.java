package org.hestiastore.indextools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

final class InitTargetConfigCommand {

    int run(final String[] args, final PrintStream out, final PrintStream err) {
        final Options options = new Options();
        options.addOption(Option.builder().longOpt("input").hasArg()
                .desc("Export directory used as the source for the template.")
                .required().get());
        options.addOption(Option.builder().longOpt("output").hasArg()
                .desc("Path where the editable target-config.json template will be written.")
                .required().get());
        options.addOption(Option.builder().longOpt("index-name").hasArg()
                .desc("Optional target index name written into the generated template.")
                .get());
        options.addOption(Option.builder().longOpt("overwrite")
                .desc("Overwrite an existing output file.").get());
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
            final Path outputFile = Path.of(commandLine.getOptionValue("output"))
                    .toAbsolutePath().normalize();
            final String indexName = commandLine.getOptionValue("index-name");
            final boolean overwrite = commandLine.hasOption("overwrite");
            execute(inputDirectory, outputFile, indexName, overwrite, out);
            return 0;
        } catch (final Exception e) {
            err.println("Target config template generation failed: "
                    + e.getMessage());
            if (!(e instanceof org.apache.commons.cli.ParseException)) {
                e.printStackTrace(err);
            } else {
                printHelp(options, err);
            }
            return 1;
        }
    }

    private void execute(final Path inputDirectory, final Path outputFile,
            final String indexName, final boolean overwrite,
            final PrintStream out) throws IOException {
        final ExportBundleManifest manifest = VerifyExportCommand
                .inspect(inputDirectory);
        IndexConfigurationManifest configuration = ManifestSupport.readJson(
                inputDirectory.resolve(manifest.getConfigFileName()),
                IndexConfigurationManifest.class);
        if (indexName != null && !indexName.isBlank()) {
            configuration = IndexConfigurationMapper.withIndexName(configuration,
                    indexName);
        }
        PathSupport.prepareOutputFile(outputFile, overwrite);
        ManifestSupport.writeJson(outputFile, configuration);
        out.printf("Wrote target config template to %s.%n", outputFile);
    }

    private void printHelp(final Options options, final PrintStream out) {
        HelpSupport.printHelp(out, "hestia_index init-target-config", options);
    }
}
