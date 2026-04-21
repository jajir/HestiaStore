package org.hestiastore.indextools;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

final class HelpSupport {

    private HelpSupport() {
    }

    static boolean isHelpRequested(final String[] args) {
        if (args == null) {
            return false;
        }
        return Arrays.stream(args).anyMatch(HelpSupport::isHelpToken);
    }

    static String commandFromHelpInvocation(final String[] args) {
        if (args == null || args.length < 2 || !"help".equals(args[0])) {
            return null;
        }
        return args[1];
    }

    static void printHelp(final PrintStream out, final String commandSyntax,
            final Options options) {
        final PrintWriter writer = new PrintWriter(out, true);
        new HelpFormatter().printHelp(writer, HelpFormatter.DEFAULT_WIDTH,
                commandSyntax, null, options, HelpFormatter.DEFAULT_LEFT_PAD,
                HelpFormatter.DEFAULT_DESC_PAD, null, true);
        writer.flush();
    }

    private static boolean isHelpToken(final String argument) {
        return "-h".equals(argument) || "--help".equals(argument)
                || "help".equals(argument);
    }
}
