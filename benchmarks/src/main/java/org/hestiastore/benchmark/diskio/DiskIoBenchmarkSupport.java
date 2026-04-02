package org.hestiastore.benchmark.diskio;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Shared filesystem-backed benchmark helpers for deterministic disk I/O runs.
 */
public final class DiskIoBenchmarkSupport {

    private static final Comparator<File> REVERSE_FILE_ORDER = Comparator
            .comparing(File::getAbsolutePath).reversed();
    private static final Path JMH_TEMP_ROOT = Path.of("target", "jmh-temp");
    private static final int KEY_WIDTH = 10;

    private DiskIoBenchmarkSupport() {
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

    public static String buildSequentialKey(final int value) {
        final String raw = String.valueOf(value);
        if (raw.length() >= KEY_WIDTH) {
            return raw;
        }
        final StringBuilder padded = new StringBuilder(KEY_WIDTH);
        for (int index = raw.length(); index < KEY_WIDTH; index++) {
            padded.append('0');
        }
        return padded.append(raw).toString();
    }

    public static Long buildLongValue(final int value) {
        return Long.valueOf((value * 1_103_515_245L) ^ 0x5DEECE66DL);
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
