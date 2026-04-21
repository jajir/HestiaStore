package org.hestiastore.indextools;

import java.io.IOException;
import java.io.PrintStream;

final class CommandJsonSupport {

    private CommandJsonSupport() {
    }

    static void printJson(final PrintStream out, final Object value)
            throws IOException {
        ManifestSupport.mapper().writeValue(out, value);
        out.println();
    }
}
