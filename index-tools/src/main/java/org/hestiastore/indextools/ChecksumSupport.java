package org.hestiastore.indextools;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ChecksumSupport {

    private ChecksumSupport() {
    }

    static String sha256(final Path file) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(file)) {
                final byte[] buffer = new byte[8192];
                int read = input.read(buffer);
                while (read >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                    read = input.read(buffer);
                }
            }
            return toHex(digest.digest());
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    static void writeChecksums(final Path file,
            final Map<String, String> checksums) throws IOException {
        final List<String> lines = checksums.entrySet().stream()
                .map(entry -> entry.getValue() + "  " + entry.getKey()).toList();
        Files.write(file, lines);
    }

    static Map<String, String> readChecksums(final Path file) throws IOException {
        final LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (final String line : Files.readAllLines(file)) {
            if (line.isBlank()) {
                continue;
            }
            final String[] parts = line.split("\\s{2,}", 2);
            if (parts.length != 2) {
                throw new IOException(
                        "Invalid checksum line in " + file + ": " + line);
            }
            result.put(parts[1], parts[0]);
        }
        return result;
    }

    static String toHex(final byte[] bytes) {
        final StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (final byte value : bytes) {
            builder.append(String.format("%02x", value));
        }
        return builder.toString();
    }
}
