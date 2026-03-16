package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;

final class SegmentIndexBenchmarkSupport {

    static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final Comparator<File> REVERSE_FILE_ORDER = Comparator
            .comparing(File::getAbsolutePath).reversed();
    private static final Path JMH_TEMP_ROOT = Path.of("target", "jmh-temp");

    private SegmentIndexBenchmarkSupport() {
    }

    static File createTempDir(final String prefix) throws IOException {
        Files.createDirectories(JMH_TEMP_ROOT);
        return Files.createTempDirectory(JMH_TEMP_ROOT, prefix).toFile();
    }

    static IndexConfigurationBuilder<Integer, String> baseBuilder(
            final String name) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withName(name)//
                .withContextLoggingEnabled(false);
    }

    static void addIntegrityAndCompressionFilters(
            final IndexConfigurationBuilder<Integer, String> builder,
            final boolean snappy) {
        builder.addEncodingFilter(new ChunkFilterCrc32Writing())
                .addEncodingFilter(new ChunkFilterMagicNumberWriting());
        builder.addDecodingFilter(new ChunkFilterMagicNumberValidation())
                .addDecodingFilter(new ChunkFilterCrc32Validation());
        if (snappy) {
            builder.addEncodingFilter(new ChunkFilterSnappyCompress());
            builder.withDecodingFilters(List.of(
                    new ChunkFilterMagicNumberValidation(),
                    new ChunkFilterSnappyDecompress(),
                    new ChunkFilterCrc32Validation()));
        }
    }

    static String buildFixedWidthValue(final String prefix, final int key,
            final int valueLength, final char fillChar) {
        final String valuePrefix = prefix + key + '-';
        final int suffixLength = Math.max(0, valueLength - valuePrefix.length());
        return valuePrefix + Character.toString(fillChar).repeat(suffixLength);
    }

    static void deleteRecursively(final File file) {
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

    static void awaitCondition(final BooleanSupplier condition,
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

    static void copyDirectory(final Path source, final Path target)
            throws IOException {
        Files.createDirectories(target);
        try (var paths = Files.walk(source)) {
            for (final Path sourcePath : paths.toList()) {
                final Path relative = source.relativize(sourcePath);
                final Path targetPath = target.resolve(relative);
                if (Files.isDirectory(sourcePath)) {
                    Files.createDirectories(targetPath);
                } else {
                    Files.copy(sourcePath, targetPath,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }
}
