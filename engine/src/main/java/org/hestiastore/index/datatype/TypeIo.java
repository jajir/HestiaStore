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
        int readOffset = 0;
        final int requiredBytes = validatedDestination.length;
        while (readOffset < requiredBytes) {
            final int read = validatedReader.read(validatedDestination,
                    readOffset, requiredBytes - readOffset);
            if (read < 0) {
                if (readOffset == 0) {
                    return false;
                }
                throw new IndexException(String.format(
                        "Expected '%s' bytes but reached EOF after '%s' bytes.",
                        requiredBytes, readOffset));
            }
            if (read == 0) {
                throw new IndexException(String.format(
                        "Expected '%s' bytes but reader returned 0 bytes at offset '%s'.",
                        requiredBytes, readOffset));
            }
            readOffset += read;
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
