package org.hestiastore.indextools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

final class PathSupport {

    private PathSupport() {
    }

    static void ensureEmptyDirectory(final Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
            return;
        }
        try (Stream<Path> listing = Files.list(directory)) {
            if (listing.findAny().isPresent()) {
                throw new IOException(
                        "Directory is not empty: " + directory.toAbsolutePath());
            }
        }
    }

    static void prepareOutputDirectory(final Path directory,
            final boolean overwrite) throws IOException {
        if (Files.exists(directory) && overwrite) {
            deleteRecursively(directory);
        }
        ensureEmptyDirectory(directory);
    }

    static void prepareOutputFile(final Path file, final boolean overwrite)
            throws IOException {
        final Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (Files.exists(file)) {
            if (!overwrite) {
                throw new IOException(
                        "File already exists: " + file.toAbsolutePath());
            }
            Files.delete(file);
        }
    }

    static void deleteRecursively(final Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(directory)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (final IOException e) {
                    throw new IllegalStateException(
                            "Unable to delete path: " + path, e);
                }
            });
        }
    }
}
