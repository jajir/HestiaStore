package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.IntFunction;

import org.hestiastore.benchmark.BenchmarkFileSupport;
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
import org.hestiastore.index.segmentindex.SegmentIndex;

final class SegmentIndexBenchmarkSupport {

    static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    private SegmentIndexBenchmarkSupport() {
    }

    static File createTempDir(final String prefix) throws IOException {
        return BenchmarkFileSupport.createTempDir(prefix);
    }

    static IndexConfigurationBuilder<Integer, String> baseBuilder(
            final String name) {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(KEY_DESCRIPTOR)
                        .valueTypeDescriptor(VALUE_DESCRIPTOR).name(name))//
                .logging(logging -> logging.contextEnabled(false));
    }

    static void addIntegrityAndCompressionFilters(
            final IndexConfigurationBuilder<Integer, String> builder,
            final boolean snappy) {
        builder.filters(filters -> {
            filters.addEncodingFilter(new ChunkFilterCrc32Writing())
                    .addEncodingFilter(new ChunkFilterMagicNumberWriting());
            filters.addDecodingFilter(new ChunkFilterMagicNumberValidation())
                    .addDecodingFilter(new ChunkFilterCrc32Validation());
            if (snappy) {
                filters.addEncodingFilter(new ChunkFilterSnappyCompress());
                filters.decodingFilters(List.of(
                        new ChunkFilterMagicNumberValidation(),
                        new ChunkFilterSnappyDecompress(),
                        new ChunkFilterCrc32Validation()));
            }
        });
    }

    static String buildFixedWidthValue(final String prefix, final int key,
            final int valueLength, final char fillChar) {
        final String valuePrefix = prefix + key + '-';
        final int suffixLength = Math.max(0, valueLength - valuePrefix.length());
        return valuePrefix + Character.toString(fillChar).repeat(suffixLength);
    }

    static void populateSequential(final SegmentIndex<Integer, String> index,
            final int keyCount, final int flushBatchSize,
            final IntFunction<String> valueBuilder) {
        int pending = 0;
        for (int key = 0; key < keyCount; key++) {
            index.put(Integer.valueOf(key), valueBuilder.apply(key));
            pending++;
            if (pending >= flushBatchSize) {
                index.flushAndWait();
                pending = 0;
            }
        }
        if (pending > 0) {
            index.flushAndWait();
        }
    }

    static void deleteRecursively(final File file) {
        BenchmarkFileSupport.deleteRecursively(file);
    }

    static void awaitCondition(final java.util.function.BooleanSupplier condition,
            final long timeoutMillis, final String message) {
        BenchmarkFileSupport.awaitCondition(condition, timeoutMillis, message);
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
