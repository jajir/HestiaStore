package org.hestiastore.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Shared benchmark filesystem and wait helpers.
 */
public final class BenchmarkFileSupport {

    private static final Comparator<File> REVERSE_FILE_ORDER = Comparator
            .comparing(File::getAbsolutePath).reversed();
    private static final Path JMH_TEMP_ROOT = Path.of("target", "jmh-temp");

    private BenchmarkFileSupport() {
    }

    public static File createTempDir(final String prefix) throws IOException {
        Files.createDirectories(JMH_TEMP_ROOT);
        return Files.createTempDirectory(JMH_TEMP_ROOT, prefix).toFile();
    }

    public static void deleteRecursively(final File file) {
        if (file == null || !file.exists()) {
            return;
        }
        final File[] children = file.listFiles();
        if (children != null) {
            java.util.Arrays.sort(children, REVERSE_FILE_ORDER);
            for (final File child : children) {
                deleteRecursively(child);
            }
        }
        if (!file.delete()) {
            throw new IllegalStateException(
                    "Unable to delete benchmark temp path: "
                            + file.getAbsolutePath());
        }
    }

    public static void awaitCondition(final BooleanSupplier condition,
            final long timeoutMillis, final String message) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(message, e);
            }
        }
        if (!condition.getAsBoolean()) {
            throw new IllegalStateException(message);
        }
    }
}
