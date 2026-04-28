package org.hestiastore.indextools;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.commons.cli.help.TextHelpAppendable;

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
        final TextHelpAppendable appendable = new TextHelpAppendable(writer);
        try {
            HelpFormatter.builder().setHelpAppendable(appendable).get()
                    .printHelp(commandSyntax, null, options, null, true);
        } catch (final java.io.IOException e) {
            throw new UncheckedIOException(e);
        }
        writer.flush();
    }

    private static boolean isHelpToken(final String argument) {
        return "-h".equals(argument) || "--help".equals(argument)
                || "help".equals(argument);
    }
}
