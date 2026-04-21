package org.hestiastore.indextools;

import java.io.PrintStream;
/**
 * Main entry point for the standalone offline index export/import CLI.
 */
public final class IndexTool {

    private IndexTool() {
    }

    /**
     * Launches the command-line tool.
     *
     * @param args command-line arguments
     */
    public static void main(final String[] args) {
        final int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(final String[] args, final PrintStream out,
            final PrintStream err) {
        if (args == null || args.length == 0) {
            printUsage(err);
            return 1;
        }
        final String commandFromHelpInvocation = HelpSupport
                .commandFromHelpInvocation(args);
        if (commandFromHelpInvocation != null) {
            return run(new String[] { commandFromHelpInvocation, "--help" }, out,
                    err);
        }
        final String command = args[0];
        final String[] commandArgs = java.util.Arrays.copyOfRange(args, 1,
                args.length);
        return switch (command) {
        case "export" -> new ExportCommand().run(commandArgs, out, err);
        case "import" -> new ImportCommand().run(commandArgs, out, err);
        case "init-target-config" -> new InitTargetConfigCommand().run(
                commandArgs, out, err);
        case "inspect-export" -> new InspectExportCommand().run(commandArgs,
                out, err);
        case "verify-export" -> new VerifyExportCommand().run(commandArgs, out,
                err);
        case "--help", "-h", "help" -> {
            printUsage(out);
            yield 0;
        }
        default -> {
            err.println("Unknown command: " + command);
            printUsage(err);
            yield 1;
        }
        };
    }

    private static void printUsage(final PrintStream out) {
        out.println("Usage: hestia_index <command> [options]");
        out.println();
        out.println("Commands:");
        out.println(
                "  export          Create an offline export bundle or JSONL dump.");
        out.println("  import          Create a new index from an export directory.");
        out.println(
                "  init-target-config  Write an editable target-config.json template.");
        out.println(
                "  inspect-export  Show export metadata, counts, and layout.");
        out.println(
                "  verify-export   Verify checksums and record counts in an export.");
        out.println();
        out.println("Help:");
        out.println("  hestia_index help");
        out.println("  hestia_index help <command>");
        out.println("  hestia_index <command> --help");
        out.println("  hestia_index <command> -h");
    }
}
