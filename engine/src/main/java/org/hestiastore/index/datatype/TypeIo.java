package org.hestiastore.index.datatype;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Shared I/O helpers for type readers.
 */
final class TypeIo {

    private TypeIo() {
    }

    static boolean readFullyOrNull(final FileReader reader,
            final byte[] destination) {
        final FileReader validatedReader = Vldtn.requireNonNull(reader,
                "reader");
        final byte[] validatedDestination = Vldtn.requireNonNull(destination,
                "destination");
        for (int i = 0; i < validatedDestination.length; i++) {
            final int readByte = validatedReader.read();
            if (readByte < 0) {
                if (i == 0) {
                    return false;
                }
                throw new IndexException(String.format(
                        "Expected '%s' bytes but reached EOF after '%s' bytes.",
                        validatedDestination.length, i));
            }
            validatedDestination[i] = (byte) readByte;
        }
        return true;
    }

    static void readFullyRequired(final FileReader reader,
            final byte[] destination) {
        final byte[] validatedDestination = Vldtn.requireNonNull(destination,
                "destination");
        if (!readFullyOrNull(reader, validatedDestination)) {
            throw new IndexException(String.format(
                    "Expected '%s' bytes but reached EOF before reading any data.",
                    validatedDestination.length));
        }
    }
}
